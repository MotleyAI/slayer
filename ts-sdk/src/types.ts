import type { components } from "./schema.js";

type Schemas = components["schemas"];

/** The input form: fields with defaults are optional, as in GET /models/{name} (which omits null fields). */
export type SlayerModel = Schemas["SlayerModel-Input"];
export type ModelExtension = Schemas["ModelExtension"];
export type TimeGranularity = Schemas["TimeGranularity"];
export type DatasourceConfig = Schemas["DatasourceConfig"];
export type QueryRefinement = Schemas["QueryRefinement"];
export type HealthResponse = Schemas["HealthResponse"];
export type ModelSummary = Schemas["ModelSummary"];
export type DatasourceSummary = Schemas["DatasourceSummary"];
export type StatusResponse = Schemas["StatusResponse"];
export type SaveMemoryResponse = Schemas["SaveMemoryResponse"];
export type ForgetMemoryResponse = Schemas["ForgetMemoryResponse"];
export type SearchResponse = Schemas["SearchResponse"];
export type SearchHit = Schemas["SearchHit"];
export type RootModelRecommendation = Schemas["RootModelRecommendation"];

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

/** A query object, a list of stages (the last is the result), or the name of a saved query-backed model. */
export type QueryInput = Query | Query[] | string;

export type Row = Record<string, unknown>;

export interface SlayerResponse<T extends Row = Row> extends Omit<Schemas["QueryResponse"], "data"> {
  data: T[];
}

export interface QueryOptions {
  /** Only with a saved query name. */
  refine?: QueryRefinement | Record<string, unknown>;
  /** Override the query's own variables. */
  variables?: Record<string, unknown>;
  dryRun?: boolean;
  explain?: boolean;
  signal?: AbortSignal;
}

export interface DataSourceOptions {
  dataSource?: string;
}

export interface SaveMemoryOptions {
  learning: string;
  /** Entity references, or a query to extract them from. */
  linkedEntities: string[] | Query;
  id?: string;
  description?: string;
}

export interface SearchOptions {
  entities?: string[];
  query?: Query;
  question?: string;
  datasource?: string;
  maxResults?: number;
  cypherFilter?: string;
  compact?: boolean;
}

export interface InspectOptions {
  entityType: string;
  /** One id, a same-kind batch, or omitted for a whole collection. */
  reference?: string | string[] | null;
  compact?: boolean;
  format?: string;
  numRows?: number;
  showSql?: boolean;
  sections?: string[];
  descriptionsMaxChars?: number;
}

export interface RecommendRootModelOptions {
  dataSource?: string;
  /** Forces the root when it reaches every item. */
  rootHint?: string;
}
