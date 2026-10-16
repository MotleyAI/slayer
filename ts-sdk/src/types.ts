import type { components } from "./schema.js";

type Schemas = components["schemas"];

export type SlayerModel = Schemas["SlayerModel"];
export type ModelExtension = Schemas["ModelExtension"];
export type TimeGranularity = Schemas["TimeGranularity"];
export type InspectRequest = Schemas["InspectRequest"];
export type SearchRequest = Schemas["SearchRequest"];
export type DatasourceConfig = Schemas["DatasourceConfig"];

/** A measure: a formula string, or a formula with a result name. */
export type Measure = string | { formula: string; name?: string; label?: string };

/** A dimension: a column reference such as "customers.name", or a computed expression. */
export type Dimension = string | { expression: string; name?: string };

export interface TimeDimension {
  dimension: string;
  granularity: TimeGranularity | (string & {});
  /** One date ("2026-01-01"), a pair, or a relative range such as "last 3 months". */
  date_range?: string | (string | null)[];
  label?: string;
}

export interface OrderItem {
  column: string;
  direction?: "asc" | "desc";
}

/** A SLayer query. The shape follows SLayer's REST API; unknown fields pass through. */
export interface Query extends Omit<Schemas["QueryRequest"], "measures" | "dimensions" | "time_dimensions" | "order" | "source_model"> {
  source_model?: string | ModelExtension | SlayerModel | null;
  measures?: Measure[] | null;
  dimensions?: Dimension[] | null;
  time_dimensions?: (string | TimeDimension)[] | null;
  order?: OrderItem[] | null;
}

export type Row = Record<string, unknown>;

export interface QueryResult<T extends Row = Row> extends Omit<Schemas["QueryResponse"], "data"> {
  data: T[];
}

export interface QueryOptions {
  variables?: Record<string, unknown>;
  dryRun?: boolean;
  explain?: boolean;
  signal?: AbortSignal;
}

/** A model as listed by GET /models. */
export interface ModelSummary {
  name: string;
  data_source?: string;
  description?: string | null;
  [key: string]: unknown;
}
