import { SlayerApiError } from "./errors.js";
import type {
  DataSourceOptions,
  DatasourceConfig,
  DatasourceSummary,
  ForgetMemoryResponse,
  HealthResponse,
  InspectOptions,
  ModelSummary,
  QueryInput,
  QueryOptions,
  RecommendRootModelOptions,
  RootModelRecommendation,
  Row,
  SaveMemoryOptions,
  SaveMemoryResponse,
  SearchOptions,
  SearchResponse,
  SlayerModel,
  SlayerResponse,
  StatusResponse,
} from "./types.js";

/** A static token, or a function that returns a fresh one for each request. */
export type TokenSource = string | (() => string | Promise<string>);

export interface SlayerClientOptions {
  /** Defaults to "http://localhost:5143". */
  url?: string;
  token?: TokenSource;
  headers?: Record<string, string>;
  /** Defaults to 60 000; null turns the timeout off. */
  timeoutMs?: number | null;
  fetch?: typeof fetch;
}

interface RequestInit {
  body?: unknown;
  params?: Record<string, string | undefined>;
  signal?: AbortSignal;
}

export class SlayerClient {
  readonly #options: SlayerClientOptions;
  readonly #url: string;

  constructor(options: SlayerClientOptions = {}) {
    this.#options = options;
    let url = options.url ?? "http://localhost:5143";
    while (url.endsWith("/")) url = url.slice(0, -1);
    this.#url = url;
  }

  /** A client with the same settings that sends another token, such as the current user's. */
  withToken(token: TokenSource): SlayerClient {
    return new SlayerClient({ ...this.#options, token });
  }

  async health(): Promise<HealthResponse> {
    return this.#request("GET", "/health");
  }

  /** Run a query, a list of stages, or a saved query-backed model by name. */
  async query<T extends Row = Row>(query: QueryInput, options: QueryOptions = {}): Promise<SlayerResponse<T>> {
    return this.#request("POST", "/query", { body: queryBody(query, options), signal: options.signal });
  }

  /** The SQL a query would run, without running it. */
  async sql(query: QueryInput, options: Omit<QueryOptions, "dryRun" | "explain"> = {}): Promise<string> {
    const { sql } = await this.query(query, { ...options, dryRun: true });
    if (sql == null) throw new Error("The dry run returned no SQL");
    return sql;
  }

  /** Run the database's EXPLAIN on a query. */
  async explain<T extends Row = Row>(
    query: QueryInput,
    options: Omit<QueryOptions, "dryRun" | "explain"> = {},
  ): Promise<SlayerResponse<T>> {
    return this.query<T>(query, { ...options, explain: true });
  }

  async listModels(options: DataSourceOptions = {}): Promise<ModelSummary[]> {
    return this.#request("GET", "/models", { params: { data_source: options.dataSource } });
  }

  async getModel(name: string, options: DataSourceOptions = {}): Promise<SlayerModel> {
    return this.#request("GET", `/models/${encodeURIComponent(name)}`, { params: { data_source: options.dataSource } });
  }

  async createModel(model: SlayerModel): Promise<StatusResponse> {
    return this.#request("POST", "/models", { body: model });
  }

  async deleteModel(name: string, options: DataSourceOptions = {}): Promise<void> {
    await this.#request("DELETE", `/models/${encodeURIComponent(name)}`, { params: { data_source: options.dataSource } });
  }

  async listDatasources(): Promise<DatasourceSummary[]> {
    return this.#request("GET", "/datasources");
  }

  async createDatasource(datasource: DatasourceConfig): Promise<StatusResponse> {
    return this.#request("POST", "/datasources", { body: datasource });
  }

  async getDatasourcePriority(): Promise<string[]> {
    const { priority } = await this.#request<{ priority: string[] }>("GET", "/datasources/priority");
    return priority;
  }

  async setDatasourcePriority(priority: string[]): Promise<void> {
    await this.#request("PUT", "/datasources/priority", { body: { priority } });
  }

  async saveMemory(options: SaveMemoryOptions): Promise<SaveMemoryResponse> {
    const body = {
      learning: options.learning,
      linked_entities: options.linkedEntities,
      ...(options.id !== undefined && { id: options.id }),
      ...(options.description !== undefined && { description: options.description }),
    };
    return this.#request("POST", "/memories", { body });
  }

  async forgetMemory(identifier: string | number): Promise<ForgetMemoryResponse> {
    return this.#request("DELETE", `/memories/${encodeURIComponent(String(identifier))}`);
  }

  async search(options: SearchOptions = {}): Promise<SearchResponse> {
    const body = defined({
      entities: options.entities,
      query: options.query,
      question: options.question,
      datasource: options.datasource,
      max_results: options.maxResults ?? 10,
      cypher_filter: options.cypherFilter,
      compact: options.compact ?? true,
    });
    return this.#request("POST", "/search", { body });
  }

  /** Describe one entity, a same-kind batch, or a whole collection, as text. */
  async inspect(options: InspectOptions): Promise<string> {
    const body = {
      reference: options.reference ?? null,
      entity_type: options.entityType,
      compact: options.compact ?? true,
      format: options.format ?? "markdown",
      num_rows: options.numRows ?? 3,
      show_sql: options.showSql ?? false,
      ...defined({ sections: options.sections, descriptions_max_chars: options.descriptionsMaxChars }),
    };
    const { result } = await this.#request<{ result: string }>("POST", "/inspect", { body });
    return result;
  }

  /** The query root (source_model) for a set of model.column / model.metric items, plus each item's path from it. */
  async recommendRootModel(items: string[], options: RecommendRootModelOptions = {}): Promise<RootModelRecommendation> {
    const body = { items, ...defined({ data_source: options.dataSource, root_hint: options.rootHint }) };
    return this.#request("POST", "/recommend-root-model", { body });
  }

  async #request<T>(method: string, path: string, init: RequestInit = {}): Promise<T> {
    const url = new URL(this.#url + path);
    for (const [key, value] of Object.entries(init.params ?? {})) {
      if (value !== undefined) url.searchParams.set(key, value);
    }
    const headers: Record<string, string> = { Accept: "application/json", ...this.#options.headers };
    const token = await this.#resolveToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    if (init.body !== undefined) headers["Content-Type"] = "application/json";

    const timeoutMs = this.#options.timeoutMs === undefined ? 60_000 : this.#options.timeoutMs;
    const signals = [init.signal, timeoutMs === null ? undefined : AbortSignal.timeout(timeoutMs)];
    const active = signals.filter((s): s is AbortSignal => s !== undefined);
    const response = await (this.#options.fetch ?? fetch)(url, {
      method,
      headers,
      body: init.body === undefined ? undefined : JSON.stringify(init.body),
      signal: active.length > 1 ? AbortSignal.any(active) : active[0],
    });
    const text = await response.text();
    const payload: unknown = text ? parseJson(text) : undefined;
    if (!response.ok) {
      const detail = isRecord(payload) && "detail" in payload ? payload.detail : payload ?? response.statusText;
      throw new SlayerApiError(response.status, detail);
    }
    return payload as T;
  }

  async #resolveToken(): Promise<string | undefined> {
    const { token } = this.#options;
    return typeof token === "function" ? token() : token;
  }
}

/** The POST /query body; the same rules as the Python client. */
function queryBody(query: QueryInput, options: QueryOptions): Record<string, unknown> {
  let body: Record<string, unknown>;
  if (typeof query === "string") {
    body = { name: query, ...(options.refine !== undefined && { refine: options.refine }) };
  } else if (options.refine !== undefined) {
    throw new TypeError("refine applies only to a saved query run by name");
  } else {
    body = Array.isArray(query) ? { queries: query } : { ...query };
  }
  if (options.variables && Object.keys(options.variables).length > 0) {
    body.variables = { ...(body.variables as Record<string, unknown> | null | undefined), ...options.variables };
  }
  if (options.dryRun) body.dry_run = true;
  if (options.explain) body.explain = true;
  return body;
}

function defined(values: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(values).filter(([, value]) => value !== undefined));
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}
