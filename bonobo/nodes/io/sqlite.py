"""
Resumable :abbr:`CSV to SQLite` loading.

The :class:`CsvToSqlite` node reads logical records from a fixed CSV file, sends each batch of source records through an
ordinary linear chain of transformations (the "pipeline"), and inserts the produced rows in a SQLite table.

The interesting part is that the whole operation is restartable: the progress of the load (how many logical CSV records
were committed) is stored *in the very same SQLite database* as the data, and updated in the *same transaction* as the
rows of the batch. Therefore:

* if the process dies in the middle of a batch, that batch is not visible at all after restart, and no progress was
  recorded for it, so it will be replayed in full (including every row produced by expanding transformations);
* if a batch was committed, its progress is committed too, so a restart will not rewrite it;
* a batch whose rows were all filtered out (or the final, shorter batch) still advances the progress, because progress
  counts *source records*, not output rows.

Constraints
-----------

This is meant for **fixed input files** and **deterministic transformations that carry no state across rows**. On each
start, the file is fingerprinted (size + ``sha256`` digest) and the load definition (target table, columns, header
handling, CSV dialect and the pipeline itself) is fingerprinted as well. If either changed since the last committed
batch, the load refuses to resume and raises :class:`SourceChangedError` or :class:`DefinitionChangedError` instead of
mixing old progress with new input.

Because each batch is executed as its own graph run, transformations that keep state across many records (for example
:class:`bonobo.Limit`, or a window) are reset at every batch and must not be used here. Per-batch state built with
context processors is fine.

The database must live on a filesystem that exposes a real path (SQLite needs one), which is the case for the default
local filesystem service. Records are delimited using universal newlines, so ``\\n``, ``\\r\\n`` and bare ``\\r`` all
work, and any encoding supported by Python can be used (``utf-8`` by default).

Usage
-----

.. code-block:: python

    import bonobo

    def clean(id, name):
        if not name:
            return  # filtered out
        yield int(id), name.strip().title()

    def split(id, name):
        for part in name.split():
            yield id, part

    loader = bonobo.CsvToSqlite(
        "input.csv",
        "output.db",
        "people",
        pipeline=(clean, split),
        columns=("id", "part"),
        batch_size=500,
    )

    graph = bonobo.Graph(loader)
    bonobo.run(graph)

If the database already contains committed batches for this same file and definition, the run will start after the last
committed record instead of reading the file from the beginning.

The node can also be used without a graph, by calling :meth:`CsvToSqlite.load` directly with a filesystem instance.
"""
import csv
import hashlib
import inspect
import io
import json
import logging
import sqlite3

from bonobo.config import Configurable, Option, Service, use_context, use_no_input
from bonobo.constants import BEGIN, END, NOT_MODIFIED
from bonobo.errors import UnrecoverableError
from bonobo.nodes.io.base import filesystem_path
from bonobo.structs.graphs import Graph
from bonobo.util.collections import tuple_or_const

__all__ = [
    "CsvToSqlite",
    "DefinitionChangedError",
    "LoadError",
    "LoadStateError",
    "SourceChangedError",
]

logger = logging.getLogger(__name__)

DEFAULT_STATE_TABLE = "_bonobo_csv_to_sqlite"
STATE_FORMAT_VERSION = 1

_DIGEST_CHUNK_SIZE = 1024 * 1024


class LoadError(UnrecoverableError):
    """
    Raised when a batch could not be loaded.

    It is an :class:`bonobo.errors.UnrecoverableError` on purpose: a failed batch means the load cannot safely continue,
    so the surrounding execution must stop instead of silently skipping data.
    """


class LoadStateError(LoadError):
    """Raised when the stored loading state cannot be used as-is."""


class SourceChangedError(LoadStateError):
    """Raised on resume when the source file changed since the last committed batch."""


class DefinitionChangedError(LoadStateError):
    """Raised on resume when the load definition changed since the last committed batch."""


def _quote_identifier(name):
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError("Invalid SQL identifier: {!r}.".format(name))
    return '"' + name.replace('"', '""') + '"'


def _stable_value(value):
    """Best-effort, deterministic description of a value, used for the definition fingerprint."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return {"bytes": bytes(value).hex()}
    if isinstance(value, (list, tuple)):
        return [_stable_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _stable_value(value[key]) for key in sorted(value, key=str)}
    if callable(value):
        return {
            "callable": "{}.{}".format(
                getattr(value, "__module__", "?"),
                getattr(value, "__qualname__", getattr(value, "__name__", type(value).__qualname__)),
            )
        }
    return repr(value)


def _describe_node(node):
    """Best-effort, deterministic description of a pipeline node, used for the definition fingerprint."""
    description = {
        "module": getattr(node, "__module__", None) or type(node).__module__,
        "name": getattr(node, "__qualname__", None) or getattr(node, "__name__", None) or type(node).__qualname__,
    }

    # Include a digest of the callable's source when possible, so that editing a cleaning function without renaming it
    # is detected as a definition change. This is best-effort: source may be unavailable (REPL, bytecode-only, C
    # extension) in which case we simply rely on the name.
    target = node
    if not (inspect.isfunction(node) or inspect.ismethod(node) or inspect.isbuiltin(node)):
        target = getattr(node, "__call__", node)
    try:
        source = inspect.getsource(target)
    except (OSError, TypeError):  # pragma: no cover - depends on the runtime
        pass
    else:
        description["source_digest"] = hashlib.sha256(source.encode("utf-8")).hexdigest()

    options = getattr(node, "__options__", None)
    if options is not None:
        described_options = {}
        for name, option in options:
            try:
                value = getattr(node, name)
            except Exception:  # pragma: no cover - defensive, e.g. renamed options
                continue
            if value is option:
                # A plain Configurable class, not an instance: the descriptor itself is not a value.
                continue
            described_options[name] = _stable_value(value)
        if described_options:
            description["options"] = described_options

    return description


class _LineSource:
    """Iterate over the lines of a text stream, remembering the seek cookie after each line.

    ``csv.reader`` consumes lines through ``next()``, and calling ``next()`` directly on a ``TextIOWrapper`` disables
    its ``tell()``. We therefore wrap the text stream in an object that feeds lines to ``csv.reader`` from
    ``TextIOWrapper.readline()``, so the cookie stays usable. Using ``newline=''`` gives universal-newline handling,
    so ``\\n``, ``\\r\\n`` and bare ``\\r`` all delimit records correctly, and the cookie maps to a *logical* CSV record
    even when a record spans several physical lines (quoted newlines).
    """

    def __init__(self, raw, encoding):
        self.text = io.TextIOWrapper(raw, encoding=encoding, newline="")

    def __iter__(self):
        return self

    def __next__(self):
        line = self.text.readline()
        if not line:
            raise StopIteration
        return line

    def tell(self):
        return self.text.tell()

    def seek(self, cookie):
        return self.text.seek(cookie)


_batch_context_types = None


def _get_batch_context_types():
    """Build (and cache) the execution context types used to run one batch.

    The imports are deferred to call time on purpose: this module is imported while :mod:`bonobo.nodes` is being built,
    which happens *before* :mod:`bonobo.execution.contexts.graph` finishes importing, so importing those modules at
    module level would create a circular import.
    """
    global _batch_context_types
    if _batch_context_types is None:
        from bonobo.execution.contexts.graph import GraphExecutionContext
        from bonobo.execution.contexts.node import NodeExecutionContext

        class _RecordingNodeExecutionContext(NodeExecutionContext):
            """Node execution context that remembers errors, as the base one only logs them."""

            def __init__(self, *args, **kwargs):
                super(_RecordingNodeExecutionContext, self).__init__(*args, **kwargs)
                self.errors = []
                self.fatals = []

            def error(self, exc_info, *, level=logging.ERROR):
                self.errors.append(exc_info)
                super(_RecordingNodeExecutionContext, self).error(exc_info, level=level)

            def fatal(self, exc_info, *, level=logging.CRITICAL):
                self.fatals.append(exc_info)
                super(_RecordingNodeExecutionContext, self).fatal(exc_info, level=level)

        class _BatchGraphExecutionContext(GraphExecutionContext):
            """Graph execution context used to run one batch of the pipeline, keeping track of failures."""

            NodeExecutionContextType = _RecordingNodeExecutionContext

            def failed(self):
                for node_context in self.nodes:
                    if node_context.fatals or node_context.errors or node_context.defunct:
                        return True
                return False

            def first_exc_info(self):
                for node_context in self.nodes:
                    if node_context.fatals:
                        return node_context.fatals[0]
                    if node_context.errors:
                        return node_context.errors[0]
                return None

        _batch_context_types = (_RecordingNodeExecutionContext, _BatchGraphExecutionContext)
    return _batch_context_types


@use_context
@use_no_input
class CsvToSqlite(Configurable):
    """
    Loads a CSV file into a SQLite table, in a resumable and atomic way.

    Each batch is made of ``batch_size`` logical CSV records. The pipeline is executed on the batch as a regular linear
    chain of transformations, and every row it produces is inserted in the target table. The batch's data and the
    loading progress are then committed in a single SQLite transaction. If anything fails while cleaning, generating or
    inserting, the batch is rolled back and the load stops with a :class:`LoadError`.

    .. attribute:: path

        Path of the CSV file, within the provided filesystem.

    .. attribute:: database

        Path of the SQLite database, within the provided filesystem.

    .. attribute:: table

        Name of the table to insert rows into. It is created if it does not exist yet (with untyped columns), but an
        existing table is perfectly fine as long as it has the requested columns.

    .. attribute:: columns

        Names of the target columns. Every row produced by the pipeline must have exactly this many values, in this
        order.

    .. attribute:: pipeline

        Linear chain of transformations applied to each batch. Each node receives the fields of a source record as
        positional arguments (like any bonobo node), may filter a record by returning nothing, and may expand it by
        yielding multiple rows. Nodes must be deterministic and must not carry state across records.

    .. attribute:: batch_size

        Number of *source records* per batch (and therefore per transaction).

    .. attribute:: header

        If true (the default), the first CSV record is treated as a header and is not part of the data.

    .. attribute:: fields

        Optional names of the source fields. If omitted and ``header`` is true, they are read from the header.

    .. attribute:: state_table

        Name of the table used to store loading progress. Defaults to ``_bonobo_csv_to_sqlite``.

    .. attribute:: state_key

        Optional key identifying this load in the state table. Defaults to a key derived from the source path and the
        target table. Provide a different key if you want to load the same file into the same table again from scratch.

    .. attribute:: encoding

        Encoding used to read the CSV file.

    .. attribute:: fs

        The filesystem service used to access both the CSV file and the database.

    """

    # Source and target
    path = Option(
        filesystem_path, positional=True, required=True, __doc__="Path of the CSV file within the filesystem."
    )
    database = Option(
        filesystem_path, positional=True, required=True, __doc__="Path of the SQLite database within the filesystem."
    )
    table = Option(str, positional=True, required=True, __doc__="Name of the target table.")

    columns = Option(
        tuple_or_const, required=True, __doc__="Names of the target columns, in the order produced by the pipeline."
    )
    pipeline = Option(
        tuple_or_const,
        default=(),
        required=False,
        __doc__="Linear chain of transformations applied to each batch of source records.",
    )

    batch_size = Option(int, default=1000, required=False, __doc__="Number of source records per batch.")
    header = Option(bool, default=True, required=False, __doc__="Whether the first CSV record is a header.")
    fields = Option(tuple_or_const, required=False, __doc__="Names of the source fields, if not read from a header.")

    state_table = Option(
        str, default=DEFAULT_STATE_TABLE, required=False, __doc__="Name of the table storing loading progress."
    )
    state_key = Option(str, required=False, __doc__="Optional key identifying this load in the state table.")

    encoding = Option(str, default="utf-8", required=False, __doc__="Encoding used to read the CSV file.")

    # Dialect related options, mirroring CsvHandler.
    delimiter = Option(str, default=csv.excel.delimiter, required=False)
    quotechar = Option(str, default=csv.excel.quotechar, required=False)
    escapechar = Option(str, default=csv.excel.escapechar, required=False)
    doublequote = Option(bool, default=csv.excel.doublequote, required=False)
    skipinitialspace = Option(bool, default=csv.excel.skipinitialspace, required=False)
    lineterminator = Option(str, default=csv.excel.lineterminator, required=False)
    quoting = Option(int, default=csv.excel.quoting, required=False)

    fs = Service("fs", __doc__="The filesystem instance to use.")

    def get_dialect_kwargs(self):
        return {
            "delimiter": self.delimiter,
            "quotechar": self.quotechar,
            "escapechar": self.escapechar,
            "doublequote": self.doublequote,
            "skipinitialspace": self.skipinitialspace,
            "lineterminator": self.lineterminator,
            "quoting": self.quoting,
        }

    # Configuration validation and derived values
    def _get_columns(self):
        columns = tuple_or_const(self.columns)
        if not isinstance(columns, tuple) or not len(columns):
            raise ValueError("The 'columns' option must be a non-empty list of column names.")
        if len(set(columns)) != len(columns):
            raise ValueError("The 'columns' option contains duplicate names: {!r}.".format(columns))
        for column in columns:
            _quote_identifier(column)
        return columns

    def _get_pipeline(self):
        pipeline = self.pipeline or ()
        if not isinstance(pipeline, tuple):
            pipeline = tuple_or_const(pipeline)
        for node in pipeline:
            if not callable(node):
                raise ValueError("Pipeline nodes must be callable, got {!r}.".format(node))
        return pipeline

    def _get_batch_size(self):
        try:
            batch_size = int(self.batch_size)
        except (TypeError, ValueError):
            raise ValueError("The 'batch_size' option must be an integer.")
        if batch_size < 1:
            raise ValueError("The 'batch_size' option must be greater than zero.")
        return batch_size

    def _get_state_key(self):
        if self.state_key:
            return str(self.state_key)
        return "{}::{}".format(self.path, self.table)

    @staticmethod
    def _collect_services(context, fs):
        services = {}
        parent = getattr(context, "parent", None)
        for container in (getattr(parent, "services", None), getattr(context, "services", None)):
            if not container:
                continue
            for key, value in container.items():
                if isinstance(key, str) and key.startswith("__"):
                    continue
                services[key] = value
        services["fs"] = fs
        return services

    # Filesystem helpers
    def _read_source_identity(self, fs):
        size = fs.getsize(self.path)
        digest = hashlib.sha256()
        with fs.openbin(self.path, "rb") as raw:
            while True:
                chunk = raw.read(_DIGEST_CHUNK_SIZE)
                if not chunk:
                    break
                digest.update(chunk)
        return size, digest.hexdigest()

    def _read_header(self, fs):
        with fs.openbin(self.path, "rb") as raw:
            source = _LineSource(raw, self.encoding)
            reader = csv.reader(source, **self.get_dialect_kwargs())
            try:
                return tuple(next(reader)), source.tell()
            except StopIteration:
                return (), source.tell()

    def _get_definition(self, source_fields):
        return json.dumps(
            {
                "version": STATE_FORMAT_VERSION,
                "table": self.table,
                "columns": list(self._get_columns()),
                "header": bool(self.header),
                "fields": list(source_fields) if source_fields else None,
                "dialect": self.get_dialect_kwargs(),
                "pipeline": [_describe_node(node) for node in self._get_pipeline()],
            },
            sort_keys=True,
            default=_stable_value,
        )

    # State management
    def _ensure_schema(self, connection):
        table, state_table = _quote_identifier(self.table), _quote_identifier(self.state_table)
        if table == state_table:
            raise ValueError("The target table and the state table must be different.")
        columns_ddl = ", ".join(_quote_identifier(column) for column in self._get_columns())
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute("CREATE TABLE IF NOT EXISTS {} ({})".format(table, columns_ddl))
            connection.execute(
                "CREATE TABLE IF NOT EXISTS {} ("
                '"load_key" TEXT PRIMARY KEY, '
                '"source_path" TEXT NOT NULL, '
                '"target_table" TEXT NOT NULL, '
                '"source_size" INTEGER NOT NULL, '
                '"source_digest" TEXT NOT NULL, '
                '"definition" TEXT NOT NULL, '
                '"record_offset" TEXT NOT NULL, '
                '"record_index" INTEGER NOT NULL, '
                '"rows_committed" INTEGER NOT NULL, '
                '"finished" INTEGER NOT NULL'
                ")".format(state_table)
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")

    def _read_state(self, connection, load_key):
        connection.row_factory = sqlite3.Row
        try:
            cursor = connection.execute(
                'SELECT * FROM {} WHERE "load_key" = ?'.format(_quote_identifier(self.state_table)), (load_key,)
            )
            return cursor.fetchone()
        finally:
            connection.row_factory = None

    @staticmethod
    def _encode_cookie(cookie):
        # TextIOWrapper tell() cookies are opaque values (an int, or a tuple for some newline modes) that can exceed
        # SQLite's signed 64-bit integer range, so they are stored as JSON in a TEXT column.
        return json.dumps(cookie)

    @staticmethod
    def _decode_cookie(value):
        return json.loads(value)

    def _initialize_state(self, connection, load_key, source_size, source_digest, definition, record_offset):
        connection.execute("BEGIN IMMEDIATE")
        try:
            connection.execute(
                "INSERT INTO {} "
                '("load_key", "source_path", "target_table", "source_size", "source_digest", "definition", '
                '"record_offset", "record_index", "rows_committed", "finished") '
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, 0)".format(_quote_identifier(self.state_table)),
                (
                    load_key,
                    self.path,
                    self.table,
                    source_size,
                    source_digest,
                    definition,
                    self._encode_cookie(record_offset),
                ),
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")

    def _commit_batch(
        self, connection, load_key, rows, record_offset, record_index, rows_committed, finished, expected_index
    ):
        columns_ddl = ", ".join(_quote_identifier(column) for column in self._get_columns())
        placeholders = ", ".join("?" for _ in self._get_columns())
        connection.execute("BEGIN IMMEDIATE")
        try:
            if rows:
                connection.executemany(
                    "INSERT INTO {} ({}) VALUES ({})".format(_quote_identifier(self.table), columns_ddl, placeholders),
                    rows,
                )
            cursor = connection.execute(
                "UPDATE {} SET "
                '"record_offset" = ?, "record_index" = ?, "rows_committed" = ?, "finished" = ? '
                'WHERE "load_key" = ? AND "record_index" = ?'.format(_quote_identifier(self.state_table)),
                (
                    self._encode_cookie(record_offset),
                    record_index,
                    rows_committed,
                    1 if finished else 0,
                    load_key,
                    expected_index,
                ),
            )
            if cursor.rowcount != 1:
                # The progress moved under us, which happens if another loader is working on the same state key. Failing
                # here (and rolling back) is far safer than committing rows that someone else also committed.
                raise LoadStateError(
                    "The loading state for {!r} was modified by another process while this batch was being committed. "
                    "Only one loader may run for a given state key at a time.".format(load_key)
                )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")

    # Batch execution
    def _run_pipeline(self, rows, services, source_fields=None):
        columns = self._get_columns()
        collected = []

        def collect(*row):
            collected.append(tuple(row))

        _, batch_context_type = _get_batch_context_types()
        pipeline = self._get_pipeline()
        graph = Graph(*pipeline, collect)
        context = batch_context_type(graph, services=services)
        try:
            context.start()
            if pipeline and source_fields:
                # Behave like a regular reader would: name the fields entering the first transformation so that
                # field-aware nodes (MapFields, Rename, OrderFields, ...) can be used in the pipeline.
                context[0].set_input_fields(source_fields)
            context.write(BEGIN, *rows, END)
            context.loop()
        except LoadError:
            raise
        except Exception as exc:
            raise LoadError("The pipeline failed before producing the batch: {}.".format(exc)) from exc
        finally:
            self._stop_batch_context(context)

        if context.failed():
            exc_info = context.first_exc_info()
            exc = exc_info[1] if exc_info else None
            raise LoadError("The pipeline failed while cleaning a batch.") from exc

        expected = len(columns)
        for row in collected:
            if len(row) != expected:
                raise LoadError(
                    "The pipeline produced a row of length {} but {} columns are expected: {!r}.".format(
                        len(row), expected, row
                    )
                )

        return collected

    @staticmethod
    def _stop_batch_context(context):
        """Stop the nodes of a batch context, tolerating nodes that never started (setup failure).

        We cannot rely on ``GraphExecutionContext.stop()`` here because it stops every node unconditionally, which
        raises for nodes that did not reach their ``start()`` (including the internal collector). Teardown errors must
        also not mask the error that caused the batch to fail.
        """
        for node_context in context.nodes:
            if not node_context.started or node_context.stopped:
                continue
            try:
                node_context.stop()
            except Exception:  # pragma: no cover - defensive
                logger.debug("Error while stopping a batch node context.", exc_info=True)

    def _resume_from_state(self, state, source_size, source_digest, definition):
        if state["source_path"] != self.path or state["target_table"] != self.table:
            raise DefinitionChangedError(
                "The stored loading state was created for another source or target ({} -> {}), not ({} -> {}). "
                "Refusing to resume.".format(
                    state["source_path"], state["target_table"], self.path, self.table
                )
            )
        if state["source_size"] != source_size or state["source_digest"] != source_digest:
            raise SourceChangedError(
                "The source file {!r} changed since the last committed batch (size/digest mismatch). Refusing to "
                "resume. Use another state_key (or remove the state row) to load the new file from scratch.".format(
                    self.path
                )
            )
        if state["definition"] != definition:
            raise DefinitionChangedError(
                "The load definition changed since the last committed batch (target table, columns, CSV dialect, "
                "header or pipeline). Refusing to resume. Use another state_key (or remove the state row) to load from "
                "scratch."
            )
        return (
            self._decode_cookie(state["record_offset"]),
            int(state["record_index"]),
            int(state["rows_committed"]),
        )

    def load(self, fs, *, services=None):
        """
        Perform the load and return a small summary dictionary.

        The summary contains ``source_path``, ``table``, ``record_index`` (number of committed source records),
        ``rows_committed`` (total rows in the target table written by this loader) and ``finished``.

        :param fs: filesystem instance used to access the CSV file and the database.
        :param services: optional services mapping given to the pipeline nodes.
        :return: dict
        """
        self._get_columns()
        batch_size = self._get_batch_size()
        source_size, source_digest = self._read_source_identity(fs)

        if self.header:
            header_fields, header_offset = self._read_header(fs)
        else:
            header_fields, header_offset = (), 0
        source_fields = tuple_or_const(self.fields) if self.fields else header_fields
        definition = self._get_definition(source_fields)

        load_key = self._get_state_key()
        connection = sqlite3.connect(fs.getsyspath(self.database), isolation_level=None, timeout=30)
        try:
            self._ensure_schema(connection)
            state = self._read_state(connection, load_key)

            if state is None:
                record_offset, record_index, rows_committed = header_offset, 0, 0
                self._initialize_state(connection, load_key, source_size, source_digest, definition, record_offset)
            else:
                record_offset, record_index, rows_committed = self._resume_from_state(
                    state, source_size, source_digest, definition
                )
                if state["finished"]:
                    logger.info(
                        "Load of %r into %r is already complete (%s records, %s rows). Nothing to do.",
                        self.path,
                        self.table,
                        record_index,
                        rows_committed,
                    )
                    return {
                        "source_path": self.path,
                        "table": self.table,
                        "record_index": record_index,
                        "rows_committed": rows_committed,
                        "finished": True,
                    }
                logger.info(
                    "Resuming load of %r into %r after record %s.",
                    self.path,
                    self.table,
                    record_index,
                )

            pipeline_services = dict(services or {})
            pipeline_services["fs"] = fs
            finished = False

            with fs.openbin(self.path, "rb") as raw:
                source = _LineSource(raw, self.encoding)
                source.seek(record_offset)
                reader = csv.reader(source, **self.get_dialect_kwargs())

                while not finished:
                    batch = []
                    batch_offset = record_offset
                    while len(batch) < batch_size:
                        try:
                            record = next(reader)
                        except StopIteration:
                            finished = True
                            break
                        batch.append(tuple(record))
                        batch_offset = source.tell()

                    rows = (
                        self._run_pipeline(batch, pipeline_services, source_fields)
                        if batch
                        else []
                    )
                    next_index = record_index + len(batch)
                    next_rows = rows_committed + len(rows)
                    try:
                        self._commit_batch(
                            connection,
                            load_key,
                            rows,
                            batch_offset,
                            next_index,
                            next_rows,
                            finished,
                            record_index,
                        )
                    except LoadStateError:
                        raise
                    except Exception as exc:
                        raise LoadError(
                            "Could not commit the batch ending after record {}: {}.".format(next_index, exc)
                        ) from exc

                    record_offset, record_index, rows_committed = batch_offset, next_index, next_rows

                    if batch:
                        logger.debug(
                            "Committed %s source records (%s rows) for %r, %s records done in total.",
                            len(batch),
                            len(rows),
                            self.path,
                            record_index,
                        )

            return {
                "source_path": self.path,
                "table": self.table,
                "record_index": record_index,
                "rows_committed": rows_committed,
                "finished": True,
            }
        finally:
            connection.close()

    def __call__(self, context, *, fs):
        self.load(fs, services=self._collect_services(context, fs))
        return NOT_MODIFIED
