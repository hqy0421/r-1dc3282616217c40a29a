Resumable loading
=================

Long loads fail. A process gets killed, a transformation raises on one bad record, the machine reboots. With most
loaders, the only recovery is to start over from the beginning of the file.

|bonobo| ships a resumable :abbr:`CSV to SQLite` loader, :class:`bonobo.CsvToSqlite`, which commits its progress together
with the data so that a restart continues where the last successful batch ended.

How it works
::::::::::::

Input is read from a fixed CSV file as **logical records**, not lines. A record may span several lines when a field
contains a quoted newline, and it is still counted, and resumed, as one record. Universal newlines are supported, so
``\n``, ``\r\n`` and bare ``\r`` all delimit records.

Records are grouped in batches of ``batch_size`` source records. For each batch:

1. the batch is pushed through a regular, linear chain of transformations (the ``pipeline``);
2. each record may be **filtered out** (the transformation returns nothing) or **expanded** into several rows (the
   transformation yields several values);
3. every produced row is inserted into the target table;
4. the number of committed source records *and* the rows are committed **in the same SQLite transaction**.

If cleaning, generation or insertion fails for any record of the batch, the transaction is rolled back. Nothing from
that batch is visible, and no progress is recorded for it, so the batch will be replayed in full on the next run. In
particular, a record that expanded into several rows before a later record of the same batch failed does not leave
those rows behind.

Progress counts **source records**, not output rows. Therefore:

* a batch whose records were all filtered out still advances the progress;
* the final, shorter batch still advances the progress.

Restarting a load
:::::::::::::::::

Running the same loader again resumes after the last committed record. It does not rewrite committed batches, and it
does not lose expanded rows that had not been committed yet, because those rows are simply produced again from the
source records that were never committed.

.. code-block:: python

    import bonobo

    def clean(id, name):
        if not name:
            return
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

    bonobo.run(bonobo.Graph(loader))

The progress is stored in a state table next to the data, in the same database. By default the table is named
``_bonobo_csv_to_sqlite``, and the state row is keyed by the source path and the target table. You can override both with
``state_table`` and ``state_key``.

Constraints
:::::::::::

This feature is deliberately narrow, because resuming a load is only correct under some conditions.

* **Fixed input.** The file must not change between runs. On every start, the loader fingerprints the file (size and a
  ``sha256`` digest) and refuses to resume if it changed, raising :class:`bonobo.SourceChangedError`.
* **Fixed definition.** The target table, the columns, the header handling, the CSV dialect and the pipeline are
  fingerprinted too. If any of them changed, the loader refuses to resume, raising
  :class:`bonobo.DefinitionChangedError`. Editing the body of a transformation function is detected as well, on a
  best-effort basis (through the function's source).
* **Deterministic cleaning, no cross-row state.** Because each batch runs as its own execution, a transformation must
  produce the same rows for the same source record regardless of other records. Transforms that keep state across many
  records, such as :class:`bonobo.Limit` or a window, are reset at each batch and must not be used here.
* **Local database.** SQLite needs a real filesystem path, which the default local filesystem service provides.

To load a changed file, or to reload the same file from scratch, use a different ``state_key`` (or remove the state
row). The loader will then start from the beginning and insert into the existing table.

Handling failures
:::::::::::::::::

A failed batch raises :class:`bonobo.LoadError`, which is an :class:`bonobo.errors.UnrecoverableError`: the surrounding
execution stops instead of silently skipping data. The database only contains fully committed batches, so the fix is
usually to correct the cleaning function, or adjust the input, and run again.

The state-verification errors, :class:`bonobo.SourceChangedError` and :class:`bonobo.DefinitionChangedError`, both
subclass :class:`bonobo.LoadStateError` and :class:`bonobo.LoadError`. You can use them to tell "this load cannot
resume" apart from "a batch failed".
