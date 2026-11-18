import sqlite3

import pytest

from bonobo import CsvToSqlite, DefinitionChangedError, LoadError, SourceChangedError, open_fs
from bonobo.nodes.io.sqlite import DEFAULT_STATE_TABLE


def write_csv(tmpdir, content, name="input.csv"):
    path = tmpdir.join(name)
    path.write(content)
    return name


def connect(tmpdir, name="output.db"):
    return sqlite3.connect(str(tmpdir.join(name)))


def fetch_state(connection, load_key):
    row = connection.execute(
        'SELECT "record_index", "rows_committed", "finished" FROM {} WHERE "load_key" = ?'.format(DEFAULT_STATE_TABLE),
        (load_key,),
    ).fetchone()
    return row


def make_loader(tmpdir, *, content, pipeline=(), columns=("id", "name"), batch_size=2, **kwargs):
    filename = write_csv(tmpdir, content)
    fs = open_fs(str(tmpdir))
    loader = CsvToSqlite(
        filename,
        "output.db",
        "people",
        pipeline=pipeline,
        columns=columns,
        batch_size=batch_size,
        **kwargs
    )
    return fs, loader


def upper(id, name):
    yield int(id), name.upper()


def drop_carol(id, name):
    if name == "carol":
        return
    yield int(id), name


def explode(id, name):
    yield int(id), name, "x"
    yield int(id), name, "y"


DATA = "id,name\n1,alice\n2,bob\n3,carol\n4,dave\n5,eve\n"


###
# Basic behaviour
###


def test_load_all_records(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,))

    summary = loader.load(fs)

    assert summary["record_index"] == 5
    assert summary["rows_committed"] == 5
    assert summary["finished"] is True

    with connect(tmpdir) as connection:
        assert connection.execute("SELECT id, name FROM people ORDER BY id").fetchall() == [
            (1, "ALICE"),
            (2, "BOB"),
            (3, "CAROL"),
            (4, "DAVE"),
            (5, "EVE"),
        ]


def test_filtered_records_do_not_produce_rows_but_advance_progress(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(drop_carol,))

    summary = loader.load(fs)

    assert summary["record_index"] == 5
    assert summary["rows_committed"] == 4
    with connect(tmpdir) as connection:
        ids = [row[0] for row in connection.execute("SELECT id FROM people ORDER BY id")]
        assert ids == [1, 2, 4, 5]


def test_expanded_rows_are_all_inserted(tmpdir):
    fs, loader = make_loader(
        tmpdir, content=DATA, pipeline=(upper, explode), columns=("id", "name", "part")
    )

    summary = loader.load(fs)

    assert summary["record_index"] == 5
    assert summary["rows_committed"] == 10
    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 10
        assert connection.execute(
            "SELECT name, part FROM people WHERE id = 3 ORDER BY part"
        ).fetchall() == [("CAROL", "x"), ("CAROL", "y")]


def test_fully_filtered_batch_still_advances(tmpdir):
    # Batch size 2: the only record of the second batch is filtered out, so it produces zero rows but must still move
    # the progress forward, otherwise the batch would be replayed forever.
    content = "id,name\n1,alice\n2,bob\n3,carol\n"
    fs, loader = make_loader(tmpdir, content=content, pipeline=(drop_carol,), batch_size=2)

    summary = loader.load(fs)

    assert summary["record_index"] == 3
    assert summary["rows_committed"] == 2
    with connect(tmpdir) as connection:
        assert fetch_state(connection, "input.csv::people") == (3, 2, 1)


def test_final_short_batch_advances(tmpdir):
    # batch_size (3) does not divide the number of records (5), so the last batch is short and must still commit.
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,), batch_size=3)

    summary = loader.load(fs)

    assert summary["record_index"] == 5
    assert summary["rows_committed"] == 5
    with connect(tmpdir) as connection:
        assert fetch_state(connection, "input.csv::people") == (5, 5, 1)


def test_headerless_csv_with_explicit_fields(tmpdir):
    content = "1,alice\n2,bob\n"
    fs, loader = make_loader(tmpdir, content=content, pipeline=(upper,), header=False, fields=("id", "name"))

    summary = loader.load(fs)

    assert summary["record_index"] == 2
    assert summary["rows_committed"] == 2


def test_source_fields_are_exposed_to_field_aware_transformations(tmpdir):
    from bonobo.nodes.basics import MapFields

    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(MapFields(str.upper),))

    loader.load(fs)

    with connect(tmpdir) as connection:
        assert connection.execute("SELECT name FROM people WHERE id = '1'").fetchone() == ("ALICE",)


def test_quoted_multiline_records_are_read_as_single_records(tmpdir):
    content = 'id,name\n1,"alice\nsmith"\n2,bob\n'
    fs, loader = make_loader(tmpdir, content=content, pipeline=(upper,))

    summary = loader.load(fs)

    assert summary["record_index"] == 2
    with connect(tmpdir) as connection:
        assert connection.execute("SELECT name FROM people WHERE id = 1").fetchone() == ("ALICE\nSMITH",)


@pytest.mark.parametrize(
    "content",
    [
        "id,name\r\n1,alice\r\n2,bob\r\n3,carol\r\n",
        "id,name\r1,alice\r2,bob\r3,carol\r",
    ],
    ids=["crlf", "cr"],
)
def test_alternative_line_endings(tmpdir, content):
    fs, loader = make_loader(tmpdir, content=content, pipeline=(upper,))

    summary = loader.load(fs)

    assert summary["record_index"] == 3
    with connect(tmpdir) as connection:
        assert connection.execute("SELECT id, name FROM people ORDER BY id").fetchall() == [
            (1, "ALICE"),
            (2, "BOB"),
            (3, "CAROL"),
        ]


def test_resume_with_crlf_line_endings(tmpdir):
    state = {"fail": True}

    def sometimes_fail(id, name):
        if state["fail"] and name == "dave":
            raise ValueError("boom")
        yield int(id), name.upper()

    content = "id,name\r\n1,alice\r\n2,bob\r\n3,carol\r\n4,dave\r\n5,eve\r\n"
    fs, loader = make_loader(tmpdir, content=content, pipeline=(sometimes_fail,), batch_size=2)

    with pytest.raises(LoadError):
        loader.load(fs)

    state["fail"] = False
    summary = loader.load(fs)

    assert summary["record_index"] == 5
    with connect(tmpdir) as connection:
        rows = connection.execute("SELECT id, name FROM people ORDER BY id").fetchall()
        assert rows == [(1, "ALICE"), (2, "BOB"), (3, "CAROL"), (4, "DAVE"), (5, "EVE")]


###
# Failure and rollback
###


def test_failed_batch_rolls_back_all_its_rows(tmpdir):
    # Records 3 and 4 form the second batch (batch_size=2). The pipeline crashes on record 4, after record 3 already
    # produced rows, so the whole batch must be rolled back, including record 3's rows.
    def fail_on_dave(id, name):
        if name == "dave":
            raise ValueError("boom on dave")
        yield int(id), name.upper()

    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(fail_on_dave,), batch_size=2)

    with pytest.raises(LoadError) as excinfo:
        loader.load(fs)
    assert isinstance(excinfo.value.__cause__, ValueError)

    with connect(tmpdir) as connection:
        # Only the first batch (records 1, 2) is committed.
        assert connection.execute("SELECT id FROM people ORDER BY id").fetchall() == [(1,), (2,)]
        assert fetch_state(connection, "input.csv::people") == (2, 2, 0)


def test_failed_expansion_does_not_leave_partial_rows(tmpdir):
    # The second batch crashes while expanding record 4. None of the expanded rows of that batch may survive.
    def fail_on_dave(id, name):
        if name == "dave":
            raise ValueError("boom on dave")
        yield int(id), name.upper(), "x"
        yield int(id), name.upper(), "y"

    fs, loader = make_loader(
        tmpdir, content=DATA, pipeline=(fail_on_dave,), columns=("id", "name", "part"), batch_size=2
    )

    with pytest.raises(LoadError):
        loader.load(fs)

    with connect(tmpdir) as connection:
        # Records 1 and 2 only, two rows each.
        assert connection.execute("SELECT id, part FROM people ORDER BY id, part").fetchall() == [
            (1, "x"),
            (1, "y"),
            (2, "x"),
            (2, "y"),
        ]


def test_restart_after_failure_does_not_duplicate_or_skip(tmpdir):
    state = {"fail": True}

    def sometimes_fail(id, name):
        if state["fail"] and name == "dave":
            raise ValueError("boom on dave")
        if name == "carol":
            return
        yield int(id), name.upper(), "x"
        yield int(id), name.upper(), "y"

    content = DATA + "6,frank\n"
    fs, loader = make_loader(
        tmpdir, content=content, pipeline=(sometimes_fail,), columns=("id", "name", "part"), batch_size=2
    )

    with pytest.raises(LoadError):
        loader.load(fs)

    state["fail"] = False
    # Build a fresh loader instance, as a new process would: resuming must not depend on any in-memory state.
    resumed = CsvToSqlite(
        "input.csv",
        "output.db",
        "people",
        pipeline=(sometimes_fail,),
        columns=("id", "name", "part"),
        batch_size=2,
    )
    summary = resumed.load(fs)

    assert summary["finished"] is True
    # 6 records minus carol = 5 records, each expanded to 2 rows.
    assert summary["rows_committed"] == 10
    with connect(tmpdir) as connection:
        rows = connection.execute("SELECT id, name, part FROM people ORDER BY id, part").fetchall()
        assert len(rows) == 10
        assert len(set(rows)) == 10
        assert {row[0] for row in rows} == {1, 2, 4, 5, 6}
        # No duplicated (id, part) pairs.
        duplicates = connection.execute(
            "SELECT id, part, COUNT(*) FROM people GROUP BY id, part HAVING COUNT(*) > 1"
        ).fetchall()
        assert duplicates == []


def test_wrong_row_width_is_reported_as_load_error(tmpdir):
    def wrong(id, name):
        yield int(id), name, "unexpected"

    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(wrong,), columns=("id", "name"))

    with pytest.raises(LoadError):
        loader.load(fs)

    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 0


def test_unrecoverable_error_in_pipeline_is_reported_as_load_error(tmpdir):
    from bonobo.errors import UnrecoverableError

    def boom(id, name):
        raise UnrecoverableError("do not continue")

    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(boom,))

    with pytest.raises(LoadError) as excinfo:
        loader.load(fs)
    assert isinstance(excinfo.value.__cause__, UnrecoverableError)

    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 0


def test_generator_failing_mid_expansion_leaves_no_partial_rows(tmpdir):
    def partial_expand(id, name):
        yield int(id), name, "x"
        if name == "bob":
            raise ValueError("halfway through bob")
        yield int(id), name, "y"

    fs, loader = make_loader(
        tmpdir, content=DATA, pipeline=(partial_expand,), columns=("id", "name", "part"), batch_size=10
    )

    with pytest.raises(LoadError):
        loader.load(fs)

    with connect(tmpdir) as connection:
        # Everything is in the first (and only) batch, so nothing at all is committed.
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 0


def test_insert_failure_rolls_back_the_batch(tmpdir):
    # Pre-create the target table with a NOT NULL constraint, so a None produced by the pipeline makes the INSERT fail.
    # The table's existence is not a problem (CREATE TABLE IF NOT EXISTS is used), the constraint just makes the insert
    # step fail after the pipeline succeeded.
    with connect(tmpdir) as connection:
        connection.execute("CREATE TABLE people (id INTEGER NOT NULL, name TEXT NOT NULL)")

    def produce_none(id, name):
        yield int(id), None

    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(produce_none,), batch_size=2)

    with pytest.raises(LoadError):
        loader.load(fs)

    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 0
        assert fetch_state(connection, "input.csv::people") == (0, 0, 0)


###
# Idempotency and resume verification
###


def test_completed_load_is_a_noop(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,))

    first = loader.load(fs)
    second = loader.load(fs)

    assert first["record_index"] == second["record_index"] == 5
    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 5


def test_changed_source_is_rejected(tmpdir):
    content = "id,name\n1,alice\n2,bob\n3,carol\n4,dave\n"

    def fail_on_close(id, name):
        if name == "dave":
            raise ValueError("stop here to leave a partial commit")
        yield int(id), name

    fs, loader = make_loader(tmpdir, content=content, pipeline=(fail_on_close,), batch_size=2)
    with pytest.raises(LoadError):
        loader.load(fs)

    # Now the file changes (same size is not guaranteed, but the digest is what matters).
    tmpdir.join("input.csv").write("id,name\n1,alice\n2,bob\n3,carol\n4,changed\n")

    with pytest.raises(SourceChangedError):
        loader.load(fs)


def test_changed_columns_are_rejected(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,))
    loader.load(fs)

    # Same source, same pipeline, but different target definition.
    other = CsvToSqlite(
        "input.csv", "output.db", "people", pipeline=(upper,), columns=("id", "name", "extra"), batch_size=2
    )
    with pytest.raises(DefinitionChangedError):
        other.load(fs)


def test_changed_pipeline_is_rejected(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,))
    loader.load(fs)

    def lower(id, name):
        yield int(id), name.lower()

    other = CsvToSqlite("input.csv", "output.db", "people", pipeline=(lower,), columns=("id", "name"), batch_size=2)
    with pytest.raises(DefinitionChangedError):
        other.load(fs)


def test_changed_dialect_is_rejected(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,))
    loader.load(fs)

    other = CsvToSqlite(
        "input.csv",
        "output.db",
        "people",
        pipeline=(upper,),
        columns=("id", "name"),
        batch_size=2,
        delimiter=";",
    )
    with pytest.raises(DefinitionChangedError):
        other.load(fs)


def test_state_key_allows_reloading_into_same_table(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,))
    loader.load(fs)

    # A different state_key restarts the load from scratch, appending the same rows again (this is what the user asked
    # for explicitly, by picking a new key).
    another = CsvToSqlite(
        "input.csv",
        "output.db",
        "people",
        pipeline=(upper,),
        columns=("id", "name"),
        batch_size=2,
        state_key="second-load",
    )
    summary = another.load(fs)

    assert summary["record_index"] == 5
    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 10


def test_load_error_is_unrecoverable():
    from bonobo.errors import UnrecoverableError

    assert issubclass(LoadError, UnrecoverableError)
    assert issubclass(SourceChangedError, LoadError)
    assert issubclass(DefinitionChangedError, LoadError)


def test_concurrent_state_change_is_detected(tmpdir):
    # A competing writer that moves the progress while a batch is being committed must not lead to duplicated data.
    # The optimistic check on record_index makes the raced batch roll back and fail loudly.
    def meddle(id, name):
        if name == "carol":
            competing = sqlite3.connect(str(tmpdir.join("output.db")), isolation_level=None)
            try:
                competing.execute('UPDATE "{}" SET "record_index" = "record_index" + 100'.format(DEFAULT_STATE_TABLE))
            finally:
                competing.close()
        yield int(id), name

    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(meddle,), batch_size=10)

    with pytest.raises(LoadError):
        loader.load(fs)

    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 0


###
# Graph integration
###


def test_load_as_part_of_a_graph(tmpdir):
    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(upper,))

    import bonobo

    graph = bonobo.Graph(loader, bonobo.count)
    context = bonobo.run(graph, services={"fs": fs})
    context.stop()

    assert context.xstatus == 0
    with connect(tmpdir) as connection:
        assert connection.execute("SELECT COUNT(*) FROM people").fetchone()[0] == 5


def test_graph_fails_on_load_error(tmpdir):
    def boom(id, name):
        raise ValueError("always fails")

    fs, loader = make_loader(tmpdir, content=DATA, pipeline=(boom,))

    import bonobo

    graph = bonobo.Graph(loader)
    context = bonobo.run(graph, services={"fs": fs})
    context.stop()

    assert context.xstatus == 70
