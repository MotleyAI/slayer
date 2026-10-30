"""Field sets of the persisted classes at the v1.4.1 tag (the previous release), with whether each forbids extra keys."""

from __future__ import annotations

#: class name -> (field names, extra="forbid").
V1_4_1_FIELD_SETS: dict[str, tuple[frozenset[str], bool]] = {
    "Aggregation": (frozenset({"description", "formula", "meta", "name", "params"}), False),
    "AggregationParam": (frozenset({"name", "sql"}), False),
    "Column": (
        frozenset(
            {
                "allowed_aggregations",
                "db_type",
                "description",
                "distinct_count",
                "filter",
                "format",
                "granularity",
                "hidden",
                "label",
                "meta",
                "name",
                "primary_key",
                "sampled",
                "sampled_values",
                "sql",
                "type",
                "unique",
            }
        ),
        False,
    ),
    "ColumnRef": (frozenset({"label", "model", "name"}), False),
    "ComputedDimension": (frozenset({"expression", "name"}), True),
    "CustomGranularity": (frozenset({"base", "multiple", "name", "origin"}), False),
    "DatasourceConfig": (
        frozenset(
            {
                "connection_name",
                "connection_string",
                "credentials_json",
                "database",
                "description",
                "granularities",
                "host",
                "name",
                "oauth_credentials_json",
                "password",
                "port",
                "postgres_schema",
                "role",
                "schema_name",
                "type",
                "username",
                "version",
                "warehouse",
            }
        ),
        False,
    ),
    "Memory": (frozenset({"created_at", "description", "entities", "id", "learning", "query", "version"}), False),
    "ModelExtension": (frozenset({"columns", "joins", "measures", "source_name"}), True),
    "ModelJoin": (
        frozenset({"cardinality", "description", "join_pairs", "join_type", "meta", "name", "target_model"}),
        False,
    ),
    "ModelMeasure": (frozenset({"description", "formula", "label", "meta", "name", "type"}), False),
    "NumberFormat": (frozenset({"precision", "symbol", "type"}), False),
    "OrderItem": (frozenset({"column", "direction", "raw_formula"}), True),
    "SlayerModel": (
        frozenset(
            {
                "access_tags",
                "aggregations",
                "columns",
                "data_source",
                "default_time_dimension",
                "description",
                "filters",
                "hidden",
                "joins",
                "measures",
                "meta",
                "name",
                "query_variables",
                "source_kind",
                "source_model_origin",
                "source_queries",
                "sql",
                "sql_table",
                "version",
            }
        ),
        False,
    ),
    "SlayerQuery": (
        frozenset(
            {
                "dimensions",
                "distinct_dimension_values",
                "filters",
                "limit",
                "main_time_dimension",
                "measures",
                "name",
                "offset",
                "order",
                "source_model",
                "time_dimensions",
                "to_many_handling",
                "variables",
                "version",
                "whole_periods_only",
            }
        ),
        True,
    ),
    "SourceModelOrigin": (frozenset({"agg_column_names", "data_source", "name", "parent"}), False),
    "TimeDimension": (frozenset({"date_range", "dimension", "granularity", "label"}), False),
}
