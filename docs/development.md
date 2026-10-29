# Development

The development guide (setup, tests, the living-architecture workflow and the pull request checklist) lives in the repository's [CONTRIBUTING.md](https://github.com/MotleyAI/slayer/blob/main/CONTRIBUTING.md).

A name newly added to the Mode-B library must not invalidate stored artefacts: a new function or function spelling needs nothing more (a model's own aggregation of that name shadows it), while any other newly reserved name (transform, date-part, granularity, aggregation) ships with a stored-upgrade migration and is pinned in `tests/test_library_growth.py`.
