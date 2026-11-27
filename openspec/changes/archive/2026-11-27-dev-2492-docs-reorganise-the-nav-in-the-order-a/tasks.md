## 1. Tests (pr-tests stage)

- [x] 1.1 Confirm Zensical's default heading slugify matches Python-Markdown's default (context7 / Zensical docs); if it differs, use Zensical's, then verify the probe count (266 anchored links, 0 broken on main) still holds.
- [x] 1.2 Add the anchor guard to `tests/test_docs_guards.py`: every relative `page.md#anchor` / same-page `#anchor` link outside code in `docs/**/*.md` and notebook Markdown cells resolves to a heading slug or explicit `id=` (design.md "Anchor guard"); verify it passes on the pre-split tree.
- [x] 1.3 Add the count guards (≥ 250 anchored links overall; separate notebook-cell minimum from the measured count with headroom) and unit tests for slug extraction (backticks, punctuation, duplicate `_1`, explicit ids) and notebook-cell extraction (inline notebook JSON, one valid + one broken link); verify they pass.
- [x] 1.4 Add `MOVED_ANCHORS` (every moved `queries.md` heading slug, subsections included → new `page.md#slug`) with tests: each is a hidden id in `queries.md`, its new target resolves, and no `docs/**/*.md` link targets `queries.md#<moved slug>` (notebooks exempt); verify they FAIL before the split.
- [x] 1.5 Add a nav-shape test: the top-level `nav` sections in `zensical.toml` are exactly Home, Get started, Connect your data, Model your data, Query your data, Agent context, Operate, Tutorials, Reference, Contributing (order not asserted); verify it FAILS before the reorganisation.

## 2. Split `docs/concepts/queries.md`

- [x] 2.1 Create `filters.md`, `cross-dataset-queries.md`, `saved-and-multistage-queries.md` with the sections in design.md's table, headings promoted one level, bodies verbatim except link targets; remove them from `queries.md`; verify by diff that only heading levels and link targets changed in moved text.
- [x] 2.2 Retarget every same-page link that crosses a file after the split (moved and remaining text) to `<file>.md#…`; verify with the anchor guard.
- [x] 2.3 Append the "Moved sections" list with hidden `<a id>` anchors for every moved slug to `queries.md`; verify the `MOVED_ANCHORS` id test passes.
- [x] 2.4 Retarget every `queries.md#<moved>` link in `docs/**/*.md` to its new home (notebooks untouched); verify the no-link-to-moved-anchor test passes and `git status` shows no `.ipynb` change.

## 3. Nav and retitles

- [x] 3.1 Rewrite the `nav` block in `zensical.toml` as in design.md "Nav", adding the three new pages; redirect map unchanged; verify the nav-coverage, nav-entry-exists and nav-shape tests pass.
- [x] 3.2 Retitle the H1 of `docs/concepts/terminology.md` to "Glossary" and of `docs/development.md` to "Contributing"; verify `tests/test_contributing_guide.py` still passes.

## 4. Finish

- [x] 4.1 Run `tests/test_docs_guards.py`, `tests/test_contributing_guide.py`, `tests/test_doc_examples.py` and ruff on the touched test file; all green.
