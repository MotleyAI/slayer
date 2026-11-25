# Development

The development guide (setup, tests, the living-architecture workflow and the pull request checklist) lives in the repository's [CONTRIBUTING.md](https://github.com/MotleyAI/slayer/blob/main/CONTRIBUTING.md).

A name newly added to the Mode-B library must not invalidate stored artefacts: a new function or function spelling needs nothing more (a dataset's own aggregation of that name shadows it), while any other newly reserved name (transform, date-part, granularity, aggregation) ships with a stored-upgrade migration registered in `MIGRATED` in `tests/test_library_growth.py`. Every newly reserved name, a function included, also needs a granularity rename gate (`RENAME_GATES`) strictly above every earlier gate, so the release steps the dataset, query and memory versions.
