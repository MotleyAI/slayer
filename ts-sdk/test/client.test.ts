import { afterEach, describe, expect, it, vi } from "vitest";
import { SlayerApiError, SlayerClient } from "../src/index.js";
import { signGatewayToken } from "../src/node.js";

const QUERY_RESPONSE = {
  data: [{ "orders.revenue": 1 }], row_count: 1, columns: ["orders.revenue"], sql: "SELECT 1",
  attributes: { dimensions: {}, measures: {} }, population: null, population_inferred: false,
  warnings: [{ kind: "normalization", rule_id: "r1", original: "a", normalized: "b", location: "measures[0]" }],
};

function mockFetch(status: number, body: unknown) {
  return vi.fn(async (_url: URL | RequestInfo, _init?: RequestInit) =>
    new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } }),
  );
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("SlayerClient defaults", () => {
  it("targets http://localhost:5143 by default", async () => {
    const fetch = mockFetch(200, { status: "ok" });
    await new SlayerClient({ fetch }).health();
    expect(String(fetch.mock.calls[0]![0])).toBe("http://localhost:5143/health");
  });

  it("strips trailing slashes from url", async () => {
    const fetch = mockFetch(200, { status: "ok" });
    await new SlayerClient({ url: "http://gw//", fetch }).health();
    expect(String(fetch.mock.calls[0]![0])).toBe("http://gw/health");
  });

  it("applies a 60 s timeout by default", async () => {
    const timeout = vi.spyOn(AbortSignal, "timeout");
    await new SlayerClient({ fetch: mockFetch(200, { status: "ok" }) }).health();
    expect(timeout).toHaveBeenCalledWith(60_000);
  });

  it("sends no signal when the timeout is turned off", async () => {
    const timeout = vi.spyOn(AbortSignal, "timeout");
    const fetch = mockFetch(200, { status: "ok" });
    await new SlayerClient({ timeoutMs: null, fetch }).health();
    expect(timeout).not.toHaveBeenCalled();
    expect(fetch.mock.calls[0]![1]?.signal).toBeUndefined();
  });

  it("fails with a TimeoutError when the server is too slow", async () => {
    const fetch = vi.fn((_url: URL | RequestInfo, init?: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(init.signal!.reason));
      }),
    );
    const error = await new SlayerClient({ timeoutMs: 20, fetch }).health().catch((e: unknown) => e);
    expect((error as Error).name).toBe("TimeoutError");
  });
});

describe("SlayerClient auth", () => {
  it("resolves a token function for every request", async () => {
    const tokens = ["t1", "t2"];
    const fetch = mockFetch(200, { status: "ok" });
    const client = new SlayerClient({ token: async () => tokens.shift()!, fetch });
    await client.health();
    await client.health();
    expect(fetch.mock.calls.map(([, init]) => new Headers(init?.headers).get("Authorization"))).toEqual([
      "Bearer t1", "Bearer t2",
    ]);
  });

  it("withToken leaves the original client's token alone", async () => {
    const fetch = mockFetch(200, { status: "ok" });
    const base = new SlayerClient({ token: "service", fetch });
    await base.withToken("user").health();
    await base.health();
    expect(fetch.mock.calls.map(([, init]) => new Headers(init?.headers).get("Authorization"))).toEqual([
      "Bearer user", "Bearer service",
    ]);
  });
});

describe("SlayerClient.query", () => {
  it("rejects refine on a query object without sending a request", async () => {
    const fetch = mockFetch(200, QUERY_RESPONSE);
    const client = new SlayerClient({ fetch });
    await expect(client.query({ source_model: "orders" }, { refine: { limit: 5 } })).rejects.toBeInstanceOf(TypeError);
    await expect(client.query([{ source_model: "orders" }], { refine: { limit: 5 } })).rejects.toBeInstanceOf(TypeError);
    expect(fetch).not.toHaveBeenCalled();
  });

  it("returns the response with wire field names", async () => {
    const client = new SlayerClient({ fetch: mockFetch(200, QUERY_RESPONSE) });
    const result = await client.query<{ "orders.revenue": number }>({ source_model: "orders", measures: ["revenue"] });
    expect(result.data[0]?.["orders.revenue"]).toBe(1);
    expect([result.row_count, result.population_inferred, result.warnings.length]).toEqual([1, false, 1]);
  });

  it("combines a caller signal with the timeout", async () => {
    const fetch = mockFetch(200, QUERY_RESPONSE);
    const controller = new AbortController();
    await new SlayerClient({ fetch }).query({ source_model: "orders" }, { signal: controller.signal });
    expect(fetch.mock.calls[0]![1]?.signal).toBeInstanceOf(AbortSignal);
  });
});

describe("SlayerApiError", () => {
  it("carries the status and detail", async () => {
    const client = new SlayerClient({ fetch: mockFetch(403, { detail: "no" }) });
    const error = await client.health().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(SlayerApiError);
    expect((error as SlayerApiError).isForbidden).toBe(true);
    expect((error as SlayerApiError).detail).toBe("no");
  });
});

describe("signGatewayToken", () => {
  it("signs a verifiable HS256 token", () => {
    const token = signGatewayToken({ sub: "u", tenant: "acme", roles: ["analyst"] }, "s".repeat(32), { ttlSeconds: 60 });
    const [, payload] = token.split(".");
    const claims = JSON.parse(Buffer.from(payload!, "base64url").toString());
    expect(claims).toMatchObject({ sub: "u", tenant: "acme", roles: ["analyst"] });
    expect(claims.exp - claims.iat).toBe(60);
  });

  it("defaults to a one-hour lifetime", () => {
    const token = signGatewayToken({ sub: "u", tenant: "acme", roles: [] }, "s".repeat(32), { now: 1000 });
    const claims = JSON.parse(Buffer.from(token.split(".")[1]!, "base64url").toString());
    expect([claims.iat, claims.exp]).toEqual([1000, 4600]);
  });

  it("rejects short secrets", () => {
    expect(() => signGatewayToken({ sub: "u", tenant: "t", roles: [] }, "short")).toThrow();
  });
});
