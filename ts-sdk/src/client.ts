import { SlayerApiError } from "./errors.js";
import type {
  DatasourceConfig,
  InspectRequest,
  ModelSummary,
  Query,
  QueryOptions,
  QueryResult,
  Row,
  SearchRequest,
  SlayerModel,
} from "./types.js";

/** A static token, or a function that returns a fresh one for each request. */
export type TokenSource = string | (() => string | Promise<string>);

export interface SlayerClientOptions {
  /** For example "http://slayer-gateway:5143". */
  baseUrl: string;
  token?: TokenSource;
  headers?: Record<string, string>;
  timeoutMs?: number;
  fetch?: typeof fetch;
}

export class SlayerClient {
  private readonly options: SlayerClientOptions;
  private readonly baseUrl: string;

  constructor(options: SlayerClientOptions) {
    this.options = options;
    let baseUrl = options.baseUrl;
    while (baseUrl.endsWith("/")) baseUrl = baseUrl.slice(0, -1);
    this.baseUrl = baseUrl;
  }

  /** A client with the same settings that sends another token, such as the current user's. */
  withToken(token: TokenSource): SlayerClient {
    return new SlayerClient({ ...this.options, token });
  }

  async health(): Promise<{ status: string }> {
    return this.request("GET", "/health");
  }

  /** Run one query. */
  async query<T extends Row = Row>(query: Query, options: QueryOptions = {}): Promise<QueryResult<T>> {
    const body = {
      ...query,
      ...(options.variables && { variables: { ...(query.variables as Record<string, unknown> | null | undefined), ...options.variables } }),
      ...(options.dryRun !== undefined && { dry_run: options.dryRun }),
      ...(options.explain !== undefined && { explain: options.explain }),
    };
    return this.request("POST", "/query", { body, signal: options.signal });
  }

  /** Run a multi-stage query. Later stages can use earlier named stages as their source; the last stage is the result. */
  async queryStages<T extends Row = Row>(queries: Query[], options: QueryOptions = {}): Promise<QueryResult<T>> {
    const body = {
      queries,
      variables: options.variables,
      dry_run: options.dryRun,
      explain: options.explain,
    };
    return this.request("POST", "/query", { body, signal: options.signal });
  }

  /** Run a saved query-backed model by name. */
  async run<T extends Row = Row>(name: string, options: QueryOptions = {}): Promise<QueryResult<T>> {
    return this.query<T>({ name }, options);
  }

  async listModels(dataSource?: string): Promise<ModelSummary[]> {
    return this.request("GET", "/models", { params: { data_source: dataSource } });
  }

  async getModel(name: string, dataSource?: string): Promise<SlayerModel> {
    return this.request("GET", `/models/${encodeURIComponent(name)}`, { params: { data_source: dataSource } });
  }

  async inspect(request: Partial<InspectRequest>): Promise<Record<string, unknown>> {
    return this.request("POST", "/inspect", { body: request });
  }

  async search(request: Partial<SearchRequest>): Promise<Record<string, unknown>> {
    return this.request("POST", "/search", { body: request });
  }

  /** Needs the write_models permission. */
  async saveModel(model: SlayerModel): Promise<void> {
    await this.request("PUT", `/models/${encodeURIComponent(model.name)}`, { body: model });
  }

  /** Needs the write_models permission. */
  async deleteModel(name: string, dataSource?: string): Promise<void> {
    await this.request("DELETE", `/models/${encodeURIComponent(name)}`, { params: { data_source: dataSource } });
  }

  /** Needs the manage_datasources permission. Secrets come back masked. */
  async listDatasources(): Promise<Partial<DatasourceConfig>[]> {
    return this.request("GET", "/datasources");
  }

  private async request<T>(
    method: string,
    path: string,
    init: { body?: unknown; params?: Record<string, string | undefined>; signal?: AbortSignal } = {},
  ): Promise<T> {
    const url = new URL(this.baseUrl + path);
    for (const [key, value] of Object.entries(init.params ?? {})) {
      if (value !== undefined) url.searchParams.set(key, value);
    }
    const headers: Record<string, string> = { Accept: "application/json", ...this.options.headers };
    const token = await this.resolveToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    if (init.body !== undefined) headers["Content-Type"] = "application/json";

    const signals = [init.signal, this.options.timeoutMs ? AbortSignal.timeout(this.options.timeoutMs) : undefined];
    const active = signals.filter((s): s is AbortSignal => s !== undefined);
    const response = await (this.options.fetch ?? fetch)(url, {
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

  private async resolveToken(): Promise<string | undefined> {
    const { token } = this.options;
    return typeof token === "function" ? token() : token;
  }
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
