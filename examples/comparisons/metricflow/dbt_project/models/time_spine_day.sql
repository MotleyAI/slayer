{{ config(materialized='table') }}
-- Day spine covering the dataset (2023-01-01 .. 2026-12-31), plus a fiscal-year (April start) column.
with days as (
    {{ dbt.date_spine('day', "make_date(2023, 1, 1)", "make_date(2027, 1, 1)") }}
)
select
    cast(date_day as date) as date_day,
    cast(date_trunc('year', cast(date_day as date) - interval 3 month) + interval 3 month as date) as fiscal_year_start
from days
