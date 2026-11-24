"""Every REQUIRED construct of the vendored Ossie expression spec: admitted with Ossie's value, or a tracked gap.

Scalars evaluate at ``orders.id = 3`` of ``tests._dev2215_fixtures`` (pending, amount 30, qty 1, created_at
2024-12-30 10:15:42, order_date 2024-12-30, shipped_at NULL, note 'c'); aggregates over all six orders.
"""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict

#: Future issue adding the missing Ossie Layer 1 constructs.
GAP = "DEV-2282"

Kind = Literal["scalar", "aggregate", "window", "reserved"]


class Row(BaseModel):
    """One construct probe; ``gap`` set means refused under strict until the gap closes."""

    model_config = ConfigDict(frozen=True)

    section: str
    form: str
    expression: str
    kind: Kind = "scalar"
    value: Any = None
    gap: Optional[str] = None


def ok(section: str, construct: str, expression: str, value: Any, *, kind: Kind = "scalar") -> Row:
    return Row(section=section, form=construct, expression=expression, value=value, kind=kind)


def gap(section: str, construct: str, expression: str, *, kind: Kind = "scalar") -> Row:
    return Row(section=section, form=construct, expression=expression, kind=kind, gap=GAP)


CORE = "Core Aggregation Functions (REQUIRED)"
STATS = "Statistical Aggregations (REQUIRED)"
PCT = "Percentile Functions (REQUIRED)"
APPROX = "Approximate Aggregations (RECOMMENDED)"
COND_AGG = "Conditional Aggregations (REQUIRED)"
FILTER = "FILTER (WHERE ...) (REQUIRED)"
AGG = "Measure Evaluation: AGG"
NOW = "Current Date/Time (REQUIRED)"
EXTRACTION = "Date/Time Extraction (REQUIRED)"
ALT = "Alternative Extraction Syntax (REQUIRED)"
TRUNC = "Date Truncation (REQUIRED)"
ARITH = "Date Arithmetic (REQUIRED)"
BUILD = "Date/Time Construction (REQUIRED)"
STRING = "String Manipulation (REQUIRED)"
SEARCH = "String Search (REQUIRED)"
PATTERN = "Pattern Matching (REQUIRED)"
BASIC = "Basic Math (REQUIRED)"
NUMERIC = "Numeric Semantics"
ADVANCED = "Advanced Math (REQUIRED)"
COMPARE = "Comparison Functions (REQUIRED)"
CASE = "CASE Expression (REQUIRED)"
COND = "Conditional Functions (REQUIRED)"
BOOLEAN = "Boolean Functions (REQUIRED)"
RANKING = "Ranking Functions (REQUIRED)"
OFFSET = "Offset Functions (REQUIRED)"
WINDOW_AGG = "Window Aggregations (REQUIRED)"
NULL_ORDER = "Window ORDER BY NULL Placement"
CAST = "CAST (REQUIRED)"
TRY_CAST = "TRY_CAST (RECOMMENDED)"
#: Prose requirements outside the REQUIRED headings.
PROSE = frozenset({AGG, NUMERIC, NULL_ORDER, TRY_CAST})

TS = "orders.created_at"
#: Row 3's ``created_at`` per extraction part.
PARTS = {"YEAR": 2024, "QUARTER": 4, "MONTH": 12, "WEEK": 1, "DAY": 30, "DAYOFYEAR": 365, "HOUR": 10, "MINUTE": 15,
         "SECOND": 42}
TRUNCATED = {"YEAR": "2024-01-01 00:00:00", "QUARTER": "2024-10-01 00:00:00", "MONTH": "2024-12-01 00:00:00",
             "WEEK": "2024-12-30 00:00:00", "DAY": "2024-12-30 00:00:00", "HOUR": "2024-12-30 10:00:00",
             "MINUTE": "2024-12-30 10:15:00", "SECOND": "2024-12-30 10:15:42"}
OVER = "OVER (ORDER BY orders.amount)"

ROWS: tuple[Row, ...] = (
    ok(CORE, "SUM", "SUM(orders.amount)", 160, kind="aggregate"),
    ok(CORE, "COUNT", "COUNT(orders.amount)", 5, kind="aggregate"),
    ok(CORE, "COUNT(*)", "COUNT(*)", 6, kind="aggregate"),
    ok(CORE, "COUNT(DISTINCT EXPR)", "COUNT(DISTINCT orders.status)", 3, kind="aggregate"),
    ok(CORE, "AVG", "AVG(orders.amount)", 32, kind="aggregate"),
    ok(CORE, "MIN", "MIN(orders.amount)", 10, kind="aggregate"),
    ok(CORE, "MAX", "MAX(orders.amount)", 60, kind="aggregate"),
    ok(STATS, "STDDEV", "STDDEV(orders.amount)", 370 ** 0.5, kind="aggregate"),
    ok(STATS, "STDDEV_POP", "STDDEV_POP(orders.amount)", 296 ** 0.5, kind="aggregate"),
    ok(STATS, "STDDEV_SAMP", "STDDEV_SAMP(orders.amount)", 370 ** 0.5, kind="aggregate"),
    ok(STATS, "VARIANCE", "VARIANCE(orders.amount)", 370, kind="aggregate"),
    ok(STATS, "VAR_POP", "VAR_POP(orders.amount)", 296, kind="aggregate"),
    ok(STATS, "VAR_SAMP", "VAR_SAMP(orders.amount)", 370, kind="aggregate"),
    ok(PCT, "MEDIAN", "MEDIAN(orders.amount)", 30, kind="aggregate"),
    ok(PCT, "PERCENTILE_CONT", "PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY orders.amount)", 20, kind="aggregate"),
    gap(PCT, "PERCENTILE_DISC", "PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY orders.amount)", kind="aggregate"),
    ok(APPROX, "APPROX_COUNT_DISTINCT", "APPROX_COUNT_DISTINCT(orders.status)", 3, kind="aggregate"),
    gap(APPROX, "APPROX_PERCENTILE", "APPROX_PERCENTILE(orders.amount, 0.5)", kind="aggregate"),
    gap(COND_AGG, "DISTINCT MODIFIER", "SUM(DISTINCT orders.qty)", kind="aggregate"),
    ok(COND_AGG, "DISTINCT MODIFIER", "COUNT(DISTINCT orders.customer_id)", 4, kind="aggregate"),
    ok(COND_AGG, "FILTERED AGGREGATION VIA CASE", "SUM(CASE WHEN orders.status = 'paid' THEN orders.amount END)", 130,
       kind="aggregate"),
    ok(COND_AGG, "FILTERED AGGREGATION VIA CASE", "COUNT(CASE WHEN orders.status = 'paid' THEN 1 END)", 4,
       kind="aggregate"),
    ok(COND_AGG, "FILTERED AGGREGATION VIA FILTER (WHERE ...)",
       "SUM(orders.amount) FILTER (WHERE orders.status = 'paid')", 130, kind="aggregate"),
    ok(COND_AGG, "FILTERED AGGREGATION VIA FILTER (WHERE ...)", "COUNT(*) FILTER (WHERE orders.status = 'paid')", 4,
       kind="aggregate"),
    ok(FILTER, "FILTER (WHERE ...)", "COUNT(DISTINCT orders.customer_id) FILTER (WHERE orders.status = 'paid')", 3,
       kind="aggregate"),
    ok(FILTER, "FILTER (WHERE ...)", "AVG(orders.amount) FILTER (WHERE orders.status = 'paid')", 32.5,
       kind="aggregate"),
    ok(FILTER, "FILTER (WHERE ...)", "MAX(orders.qty) FILTER (WHERE orders.region = 'EU')", 2, kind="aggregate"),
    Row(section=AGG, form="AGG", expression="AGG(orders.amount)", kind="reserved"),
    ok(NOW, "CURRENT_DATE", "CURRENT_DATE > orders.order_date", True),
    ok(NOW, "CURRENT_DATE", "CURRENT_DATE() > orders.order_date", True),
    ok(NOW, "CURRENT_TIMESTAMP", f"CURRENT_TIMESTAMP > {TS}", True),
    ok(NOW, "CURRENT_TIMESTAMP", f"CURRENT_TIMESTAMP() > {TS}", True),
    gap(NOW, "CURRENT_TIME", "CURRENT_TIME"),
    gap(NOW, "CURRENT_TIME", "CURRENT_TIME()"),
    *(ok(EXTRACTION, p, f"{p}({TS})", PARTS[p])
      for p in ("YEAR", "QUARTER", "MONTH", "DAY", "DAYOFYEAR", "HOUR", "MINUTE", "SECOND")),
    *(ok(ALT, f"EXTRACT({p})", f"EXTRACT({p} FROM {TS})", v) for p, v in PARTS.items()),
    *(ok(ALT, f"DATE_PART({p})", f"DATE_PART('{p.lower()}', {TS})", v) for p, v in PARTS.items()),
    *(gap(ALT, f"{fn}({p})", f"EXTRACT({p} FROM {TS})" if fn == "EXTRACT" else f"DATE_PART('{p.lower()}', {TS})")
      for p in ("DAYOFWEEK", "MILLISECOND") for fn in ("EXTRACT", "DATE_PART")),
    ok(TRUNC, "DATE_TRUNC", f"DATE_TRUNC('month', {TS})", TRUNCATED["MONTH"]),
    *(ok(TRUNC, f"DATE_TRUNC({p})", f"DATE_TRUNC('{p.lower()}', {TS})", v) for p, v in TRUNCATED.items()),
    ok(ARITH, "DATEADD", "DATEADD(day, 7, orders.order_date)", "2025-01-06"),
    ok(ARITH, "DATEADD", "DATEADD(month, -1, orders.order_date)", "2024-11-30"),
    ok(ARITH, "DATEDIFF", "DATEDIFF(day, orders.order_date, DATE '2025-01-09')", 10),
    ok(ARITH, "DATEDIFF", "DATEDIFF(month, orders.order_date, DATE '2025-03-01')", 3),
    ok(BUILD, "DATE '2024-01-15'", "DATE '2024-01-15'", "2024-01-15"),
    gap(BUILD, "TIMESTAMP_NTZ '2024-01-15 10:30:00'", "TIMESTAMP_NTZ '2024-01-15 10:30:00'"),
    gap(BUILD, "TIME '10:30:00'", "TIME '10:30:00'"),
    ok(BUILD, "CAST('2024-01-15' AS DATE)", "CAST('2024-01-15' AS DATE)", "2024-01-15"),
    ok(BUILD, "CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)", "CAST('2024-01-15 10:30:00' AS TIMESTAMP_NTZ)",
       "2024-01-15 10:30:00"),
    gap(BUILD, "CAST('10:30:00' AS TIME)", "CAST('10:30:00' AS TIME)"),
    gap(BUILD, "TO_DATE", "TO_DATE('2024-01-15')"),
    gap(BUILD, "TO_TIMESTAMP", "TO_TIMESTAMP('2024-01-15 10:30:00')"),
    ok(STRING, "CONCAT", "CONCAT(orders.status, '-', orders.note)", "pending-c"),
    ok(STRING, "||", "orders.status || orders.note", "pendingc"),
    ok(STRING, "LENGTH", "LENGTH(orders.status)", 7),
    ok(STRING, "LOWER", "LOWER('AbC')", "abc"),
    ok(STRING, "UPPER", "UPPER(orders.status)", "PENDING"),
    ok(STRING, "TRIM", "TRIM('  x  ')", "x"),
    ok(STRING, "LTRIM", "LTRIM('  x  ')", "x  "),
    ok(STRING, "RTRIM", "RTRIM('  x  ')", "  x"),
    ok(STRING, "LEFT", "LEFT(orders.status, 3)", "pen"),
    ok(STRING, "RIGHT", "RIGHT(orders.status, 3)", "ing"),
    ok(STRING, "SUBSTRING", "SUBSTRING(orders.status, 2, 3)", "end"),
    ok(STRING, "REPLACE", "REPLACE(orders.status, 'en', 'EN')", "pENding"),
    ok(STRING, "SPLIT_PART", "SPLIT_PART('a-b-c', '-', 2)", "b"),
    ok(SEARCH, "POSITION", "POSITION('d' IN orders.status)", 4),
    gap(SEARCH, "CHARINDEX", "CHARINDEX('d', orders.status)"),
    ok(SEARCH, "CONTAINS", "CONTAINS(orders.status, 'end')", True),
    gap(SEARCH, "STARTSWITH", "STARTSWITH(orders.status, 'pe')"),
    gap(SEARCH, "ENDSWITH", "ENDSWITH(orders.status, 'ing')"),
    ok(PATTERN, "LIKE", "orders.status LIKE 'pe%'", True),
    ok(PATTERN, "ILIKE", "orders.status ILIKE 'PE%'", True),
    ok(PATTERN, "REGEXP_LIKE", "REGEXP_LIKE(orders.status, '^pe')", True),
    ok(BASIC, "ABS", "ABS(-2.5)", 2.5),
    ok(BASIC, "ROUND", "ROUND(orders.amount / 7, 2)", 4.29),
    ok(BASIC, "FLOOR", "FLOOR(-2.5)", -3),
    ok(BASIC, "CEIL", "CEIL(2.1)", 3),
    ok(BASIC, "CEILING", "CEILING(2.1)", 3),
    gap(BASIC, "TRUNC", "TRUNC(2.789, 1)"),
    gap(BASIC, "TRUNCATE", "TRUNCATE(2.789, 1)"),
    ok(BASIC, "MOD", "MOD(orders.qty + 6, 4)", 3),
    ok(BASIC, "SIGN", "SIGN(-3)", -1),
    ok(NUMERIC, "ROUND TIES", "ROUND(2.5, 0)", 3),
    ok(NUMERIC, "ROUND TIES", "ROUND(-2.5, 0)", -3),
    ok(NUMERIC, "ROUND TIES", "ROUND(0.125, 2)", 0.13),
    ok(NUMERIC, "ROUND NEGATIVE DIGITS", "ROUND(1250, -2)", 1300),
    ok(NUMERIC, "ROUND NEGATIVE DIGITS", "ROUND(1234.5678, -1)", 1230),
    ok(NUMERIC, "TRUE DIVISION", "5 / 2", 2.5),
    ok(NUMERIC, "TRUE DIVISION", "orders.qty / 2", 0.5),
    ok(ADVANCED, "POWER", "POWER(2, 10)", 1024),
    ok(ADVANCED, "SQRT", "SQRT(16)", 4),
    ok(ADVANCED, "EXP", "EXP(0)", 1),
    ok(ADVANCED, "LN", "LN(1)", 0),
    ok(ADVANCED, "LOG", "LOG(2, 8)", 3),
    ok(ADVANCED, "LOG10", "LOG10(1000)", 3),
    ok(COMPARE, "GREATEST", "GREATEST(1, 5, 3)", 5),
    ok(COMPARE, "LEAST", "LEAST(4, 2, 8)", 2),
    ok(CASE, "SEARCHED CASE", "CASE WHEN orders.amount > 20 THEN 'big' ELSE 'small' END", "big"),
    ok(CASE, "SIMPLE CASE", "CASE orders.status WHEN 'pending' THEN 1 WHEN 'paid' THEN 2 ELSE 0 END", 1),
    gap(COND, "IF", "IF(orders.amount > 20, 1, 0)"),
    ok(COND, "IFF", "IFF(orders.amount > 20, 1, 0)", 1),
    ok(COND, "NULLIF", "NULLIF(orders.qty, 1)", None),
    ok(COND, "COALESCE", "COALESCE(NULL, orders.status, 'x')", "pending"),
    ok(COND, "IFNULL", "IFNULL(NULLIF(orders.status, 'pending'), 'x')", "x"),
    gap(COND, "NVL", "NVL(NULLIF(orders.status, 'pending'), 'x')"),
    gap(COND, "NVL2", "NVL2(orders.status, 'y', 'n')"),
    gap(COND, "ZEROIFNULL", "ZEROIFNULL(orders.amount)"),
    gap(COND, "NULLIFZERO", "NULLIFZERO(orders.amount)"),
    ok(BOOLEAN, "BOOLEAN", "TRUE", True),
    ok(BOOLEAN, "BOOLEAN", "FALSE", False),
    ok(BOOLEAN, "NOT", "NOT (orders.qty > 1)", True),
    ok(BOOLEAN, "AND", "orders.qty = 1 AND orders.amount > 20", True),
    ok(BOOLEAN, "OR", "orders.qty = 2 OR orders.amount > 20", True),
    *(gap(RANKING, f, f"{f}({arg}) {OVER}", kind="window") for f, arg in (
        ("ROW_NUMBER", ""), ("RANK", ""), ("DENSE_RANK", ""), ("NTILE", "2"), ("PERCENT_RANK", ""),
        ("CUME_DIST", ""))),
    *(gap(OFFSET, f, f"{f}({args}) {OVER}", kind="window") for f, args in (
        ("LAG", "orders.amount, 1, 0"), ("LEAD", "orders.amount, 1, 0"), ("FIRST_VALUE", "orders.amount"),
        ("LAST_VALUE", "orders.amount"), ("NTH_VALUE", "orders.amount, 2"))),
    gap(WINDOW_AGG, "RUNNING TOTAL", "SUM(orders.amount) OVER (ORDER BY orders.order_date)", kind="window"),
    gap(WINDOW_AGG, "RUNNING AVERAGE",
        "AVG(orders.amount) OVER (ORDER BY orders.order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)", kind="window"),
    gap(WINDOW_AGG, "PARTITION TOTALS", "SUM(orders.amount) OVER (PARTITION BY orders.region)", kind="window"),
    gap(WINDOW_AGG, "PERCENT OF TOTAL", "orders.amount / SUM(orders.amount) OVER () * 100", kind="window"),
    gap(NULL_ORDER, "DEFAULT NULL PLACEMENT", "RANK() OVER (ORDER BY orders.amount DESC)", kind="window"),
    ok(CAST, "CAST AS VARCHAR", "CAST(orders.qty AS VARCHAR)", "1"),
    ok(CAST, "CAST AS STRING", "CAST(orders.qty AS STRING)", "1"),
    ok(CAST, "CAST AS INTEGER", "CAST(2.5 AS INTEGER)", 3),
    ok(CAST, "CAST AS INT", "CAST(-2.5 AS INT)", -3),
    ok(CAST, "CAST AS BIGINT", "CAST(orders.amount / 8 AS BIGINT)", 4),
    ok(CAST, "CAST AS DECIMAL", "CAST(12.345 AS DECIMAL(10, 2))", 12.35),
    ok(CAST, "CAST AS DECIMAL", "CAST(12.5 AS DECIMAL)", 13),
    ok(CAST, "CAST AS NUMERIC", "CAST(orders.amount / 8 AS NUMERIC(10, 1))", 3.8),
    ok(CAST, "CAST AS FLOAT", "CAST(orders.qty AS FLOAT)", 1),
    ok(CAST, "CAST AS DOUBLE", "CAST('2.5' AS DOUBLE)", 2.5),
    ok(CAST, "CAST AS BOOLEAN", "CAST(orders.qty + 4 AS BOOLEAN)", True),
    ok(CAST, "CAST AS BOOLEAN", "CAST(0 AS BOOLEAN)", False),
    ok(CAST, "CAST AS DATE", "CAST('2024-01-15' AS DATE)", "2024-01-15"),
    ok(CAST, "CAST AS TIMESTAMP", "CAST('2024-01-15 10:30:00' AS TIMESTAMP)", "2024-01-15 10:30:00"),
    gap(CAST, "CAST AS TIME", "CAST('10:30:00' AS TIME)"),
    ok(CAST, "CAST AS VARCHAR", "CAST('abcdef' AS VARCHAR(3))", "abc"),
    ok(TRY_CAST, "TRY_CAST", "TRY_CAST('12' AS INTEGER)", 12),
    ok(TRY_CAST, "TRY_CAST", "TRY_CAST('x' AS INTEGER)", None),
    ok(TRY_CAST, "TRY_CAST", "TRY_CAST(2.5 AS INT)", 3),
    ok(TRY_CAST, "TRY_CAST", "TRY_CAST(5 AS BOOLEAN)", True),
)
