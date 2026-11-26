"""Spec: compatibility/ossie-compliance › the gate reads each wide-table case from its ``.ost`` script, failing closed."""

from __future__ import annotations

import contextlib
import re
from collections.abc import Generator
from decimal import Decimal
from pathlib import Path

import pytest

import tests._ossie_gate_suite as suite
from tests._dev2217_fixtures import WIDE_TABLE

PREAMBLE = ('# A synthetic case.\nconnect("retail");\nset("doubleDigits", 2);\n\n'
            'modelFile("retail_model", "retail/retail_model.ossie.yaml");\n\n')
QUERY = 'executeLower("retail_model", {\n  "metrics": ["orders.total_revenue"]\n});\n'
LOWERING = "> {_|\nSELECT\n  AGG(orders.total_revenue) AS total_revenue\n|_} : string\n"
SCRIPTS = sorted(WIDE_TABLE.glob("*.ost"))


def _table(header: list[str], *rows: list[str]) -> str:
    """A table as ost writes it."""
    widths = [max(len(cell) for cell in column) for column in zip(header, *rows)]
    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+\n"

    def line(cells: list[str]) -> str:
        return "|" + "|".join(f" {cell.ljust(w)} " for cell, w in zip(cells, widths)) + "|\n"

    return border + line(header) + border + "".join(line(r) for r in rows) + border


TABLE = _table(["total_revenue"], ["650"])


def _script(tmp_path: Path, text: str, name: str = "a01_case") -> Path:
    path = tmp_path / f"{name}.ost"
    path.write_text(text, encoding="utf-8")
    return path


def _line(text: str, marker: str, occurrence: int = 1) -> int:
    """The 1-based line of ``marker``'s ``occurrence``-th appearance."""
    at = -1
    for _ in range(occurrence):
        at = text.index(marker, at + 1)
    return text[:at].count("\n") + 1


@contextlib.contextmanager
def _refused(script: Path, line: int | None = None) -> Generator[None]:
    """The reader raises, naming ``script`` and, when given, ``line``."""
    with pytest.raises(suite.CaseScriptError) as raised:
        yield
    where = re.escape(script.name) + (rf":{line}(?!\d)" if line is not None else "")
    assert re.search(where, str(raised.value)), raised.value


# --------------------------------------------------------------------------- #
# Row and error cases
# --------------------------------------------------------------------------- #
def test_a_row_case(tmp_path: Path) -> None:
    table = _table(["name", "value"],
                   ["EAST", "350"], ["", "-0.50"], ['""', "162.50"], ['"a|b"', ""], ['" a"', "1e+16"],
                   ['"say \\"hi\\""', "2024-03-10"], ['"\\u0007x"', "true"])
    script = _script(tmp_path, PREAMBLE + QUERY + LOWERING + table)
    case = suite.read_case(script)
    assert case.name == "a01_case"
    assert case.query == {"metrics": ["orders.total_revenue"]}
    assert case.error is None
    assert case.header == ["name", "value"]
    assert case.rows == [["EAST", Decimal("350")], [None, Decimal("-0.50")], ["", Decimal("162.50")],
                         ["a|b", None], [" a", Decimal("1e+16")], ['say "hi"', "2024-03-10"], ["\x07x", "true"]]


def test_a_quoted_cell_keeps_its_text(tmp_path: Path) -> None:
    """A quoted cell keeps its text."""
    script = _script(tmp_path, PREAMBLE + QUERY + LOWERING + _table(["s"], ['"a|b"'], ['""'], [""]))
    assert suite.read_case(script).rows == [["a|b"], [""], [None]]


@pytest.mark.parametrize("line", [
    "> error: E_NO_PATH: metric tickets.ticket_count does not reach customers",
    "> error: E_NO_PATH",
], ids=["code before a colon", "code at the end of the line"])
def test_an_error_case(tmp_path: Path, line: str) -> None:
    case = suite.read_case(_script(tmp_path, PREAMBLE + QUERY + line + "\n", name="e01_case"))
    assert case.error == "E_NO_PATH"
    assert case.header == []
    assert case.rows == []


def test_data_and_model_come_from_the_script(tmp_path: Path) -> None:
    text = ('connect("stores");\nmodelFile("stores_ambiguous_model", "stores/stores_ambiguous_model.ossie.yaml");\n'
            'executeLower("stores_ambiguous_model", {"metrics": ["store_returns.total_returns"]});\n'
            + LOWERING + TABLE)
    script = _script(tmp_path, text)
    case = suite.read_case(script)
    assert case.data == "stores"
    assert case.data_path == WIDE_TABLE / "stores" / "stores.sql"
    assert case.model == "stores_ambiguous_model"
    assert case.model_path == script.parent / "stores" / "stores_ambiguous_model.ossie.yaml"


def test_upstream_skip_is_ignored(tmp_path: Path) -> None:
    """A case upstream skips still runs."""
    text = PREAMBLE + '# Not expandable yet.\nskip("https://github.com/apache/ossie/issues/814");\n' + QUERY
    case = suite.read_case(_script(tmp_path, text + LOWERING + TABLE))
    assert case.rows == [[Decimal("650")]]


# --------------------------------------------------------------------------- #
# Lexical constructs: what ends a command, what a raw string holds
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("command", [
    'skip("first;\nsecond");',
    'skip("say ""x"";\n");',
    "skip('it''s;\nok');",
    'skip("x" /* a;\n b */);',
    'skip("x" -- a;\n);',
    'skip(\n"x";\n);',
    "skip({|a;\n|});",
    "skip({_|\na;\n|_});",
    "skip({t|a |} b;\n|t});",
], ids=["double quotes", "doubled double quote", "single quotes", "block comment", "line comment", "parentheses",
        "raw string", "underscore raw string", "tagged raw string"])
def test_a_semicolon_inside_a_construct_does_not_end_a_command(tmp_path: Path, command: str) -> None:
    case = suite.read_case(_script(tmp_path, PREAMBLE + command + "\n" + QUERY + LOWERING + TABLE))
    assert case.rows == [[Decimal("650")]]


@pytest.mark.parametrize(("opening", "closing", "content"), [
    ("{|", "|}", "-- orders.c1 is SUM(amount)\n+-----+\n| x |\nSELECT 1\n"),
    ("{_|", "|_}", "\n+-----+\n| x |\nSELECT 1\n"),
    ("{a|", "|a}", "x |} y\n| x |\nSELECT 1\n"),
    ("{_ab|", "|_ab}", "\nx |_} y\n+---+\nSELECT 1\n"),
], ids=["plain", "underscore", "tagged", "underscore tagged"])
def test_a_raw_string_lowering_is_skipped(tmp_path: Path, opening: str, closing: str, content: str) -> None:
    lowering = f"> {opening}{content}{closing} : string\n"
    case = suite.read_case(_script(tmp_path, PREAMBLE + QUERY + lowering + TABLE))
    assert case.header == ["total_revenue"]
    assert case.rows == [[Decimal("650")]]


def test_a_hash_line_inside_a_command_is_part_of_it(tmp_path: Path) -> None:
    """Kept in the query, the ``#`` line makes it unreadable."""
    query = 'executeLower("retail_model", {\n# not a comment\n  "metrics": ["orders.total_revenue"]\n});\n'
    text = PREAMBLE + query + LOWERING + TABLE
    script = _script(tmp_path, text)
    with _refused(script, _line(text, "executeLower")):
        suite.read_case(script)


def test_a_semicolon_followed_by_a_comment_does_not_end_a_command(tmp_path: Path) -> None:
    text = 'connect("retail"); -- the data\n' + PREAMBLE.split("\n", 2)[2] + QUERY + LOWERING + TABLE
    script = _script(tmp_path, text)
    with _refused(script, _line(text, "connect")):
        suite.read_case(script)


# --------------------------------------------------------------------------- #
# Failing closed
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("command", ["plus(1, 2);\n> 3 : int\n", "select 1;\n" + _table(["1"], ["1"])],
                         ids=["function", "sql"])
def test_an_unknown_command_is_refused(tmp_path: Path, command: str) -> None:
    text = PREAMBLE + command + QUERY + LOWERING + TABLE
    script = _script(tmp_path, text)
    with _refused(script, _line(text, command.split("\n")[0])):
        suite.read_case(script)


def test_a_script_without_execute_lower_is_refused(tmp_path: Path) -> None:
    script = _script(tmp_path, PREAMBLE)
    with _refused(script):
        suite.read_case(script)


def test_a_second_execute_lower_is_refused(tmp_path: Path) -> None:
    text = PREAMBLE + QUERY + LOWERING + TABLE + "\n" + QUERY + LOWERING + TABLE
    script = _script(tmp_path, text)
    with _refused(script, _line(text, "executeLower", occurrence=2)):
        suite.read_case(script)


def test_an_unknown_connect_name_is_refused(tmp_path: Path) -> None:
    text = PREAMBLE.replace('connect("retail")', 'connect("scott")') + QUERY + LOWERING + TABLE
    script = _script(tmp_path, text)
    with _refused(script, _line(text, "connect")):
        suite.read_case(script)


def test_a_script_without_connect_is_refused(tmp_path: Path) -> None:
    script = _script(tmp_path, PREAMBLE.replace('connect("retail");\n', "") + QUERY + LOWERING + TABLE)
    with _refused(script):
        suite.read_case(script)


def test_a_script_without_model_file_is_refused(tmp_path: Path) -> None:
    text = PREAMBLE.split("modelFile")[0] + QUERY + LOWERING + TABLE
    script = _script(tmp_path, text)
    with _refused(script, _line(text, "executeLower")):
        suite.read_case(script)


def test_a_query_on_another_model_is_refused(tmp_path: Path) -> None:
    text = PREAMBLE + QUERY.replace('"retail_model"', '"other_model"') + LOWERING + TABLE
    script = _script(tmp_path, text)
    with _refused(script, _line(text, "executeLower")):
        suite.read_case(script)


@pytest.mark.parametrize(("table", "marker"), [
    (TABLE.rstrip("\n").rsplit("\n", 1)[0] + "\n", None),
    (TABLE.replace("+---------------+\n", "+-------x-------+\n", 1), "-x-"),
    (_table(["a", "b"], ["1", "2"]).replace("| 1 | 2 |", "| 1 | 2 | 3 |"), "| 3 |"),
    (_table(["a", "b"], ["1", "2"]).replace("| 1 | 2 |", "| 1 |"), "| 1 |"),
], ids=["no closing border", "malformed border", "row wider than the header", "row narrower than the header"])
def test_a_malformed_table_is_refused(tmp_path: Path, table: str, marker: str | None) -> None:
    text = PREAMBLE + QUERY + LOWERING + table
    script = _script(tmp_path, text)
    with _refused(script, _line(text, marker) if marker else None):
        suite.read_case(script)


@pytest.mark.parametrize("line", [
    "> error: ost: plus: expected an integer",
    "> error: E_no_path: metric",
    "> error: E_NO_PATH metric tickets.ticket_count",
    "> error:E_NO_PATH",
], ids=["not a code", "lower case", "no colon after the code", "no space after the colon"])
def test_an_error_line_without_a_code_is_refused(tmp_path: Path, line: str) -> None:
    text = PREAMBLE + QUERY + line + "\n"
    script = _script(tmp_path, text, name="e01_case")
    with _refused(script, _line(text, line)):
        suite.read_case(script)


def test_an_error_case_with_a_table_is_refused(tmp_path: Path) -> None:
    text = PREAMBLE + QUERY + "> error: E_NO_PATH: m\n" + TABLE
    script = _script(tmp_path, text, name="e01_case")
    with _refused(script, _line(text, "+--")):
        suite.read_case(script)


@pytest.mark.parametrize(("output", "marker"), [
    ("", "executeLower"),
    (LOWERING, "executeLower"),
    (TABLE, "executeLower"),
    (LOWERING + "> error: E_NO_PATH: m\n", "> error"),
], ids=["no output", "a lowering without a table", "a table without a lowering", "a lowering then an error"])
def test_any_other_output_is_refused(tmp_path: Path, output: str, marker: str) -> None:
    text = PREAMBLE + QUERY + output
    script = _script(tmp_path, text)
    with _refused(script, _line(text, marker)):
        suite.read_case(script)


@pytest.mark.parametrize(("text", "marker"), [
    (PREAMBLE + "# a comment\n| stray |\n" + QUERY + LOWERING + TABLE, "| stray"),
    (PREAMBLE + 'skip("x"));\n' + QUERY + LOWERING + TABLE, "skip"),
    (PREAMBLE + QUERY + "> {_|\nSELECT 1\n", "> {_|"),
    (PREAMBLE + 'skip("x" /* open\n' + QUERY, "skip"),
], ids=["output after a comment", "unmatched parenthesis", "unterminated raw string", "unterminated comment"])
def test_a_malformed_script_is_refused(tmp_path: Path, text: str, marker: str) -> None:
    script = _script(tmp_path, text)
    with _refused(script, _line(text, marker)):
        suite.read_case(script)


# --------------------------------------------------------------------------- #
# The vendored (or $SLAYER_OSSIE_CORPUS) case scripts
# --------------------------------------------------------------------------- #
def test_the_cases_are_the_case_scripts() -> None:
    assert SCRIPTS
    assert [case.name for case in suite.cases()] == [p.stem for p in SCRIPTS]
    assert [case.name for case in suite.row_cases()] == [p.stem for p in SCRIPTS if p.stem.startswith("a")]


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.stem)
def test_every_case_script_reads(script: Path) -> None:
    case = suite.read_case(script)
    assert case.name == script.stem
    if script.stem.startswith("e"):
        assert case.error is not None and not case.header
    else:
        assert case.error is None and case.header


def test_a_case_upstream_skips_reads_its_expected_rows() -> None:
    """A case upstream skips still runs."""
    script = WIDE_TABLE / "a21_single_path_on_ambiguous_model.ost"
    assert "\nskip(" in script.read_text(encoding="utf-8")
    case = suite.read_case(script)
    assert case.model == "stores_ambiguous_model"
    assert case.header == ["year", "total_returns"]
    assert case.rows == [[Decimal("2023"), Decimal("10")], [Decimal("2024"), Decimal("5")]]
