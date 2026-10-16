import { describe, expect, it, vi } from "vitest";
import { SlayerApiError, SlayerClient } from "../src/index.js";
import { signGatewayToken } from "../src/node.js";

function mockFetch(status: number, body: unknown) {
  return vi.fn(async (_url: URL | RequestInfo, _init?: RequestInit) =>
    new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } }),
  );
}

describe("SlayerClient", () => {
  it("posts queries with a bearer token and options", async () => {
    const fetch = mockFetch(200, { data: [{ "orders.revenue": 1 }], row_count: 1, columns: ["orders.revenue"] });
    const client = new SlayerClient({ baseUrl: "http://gw/", token: async () => "t1", fetch });
    const result = await client.query<{ "orders.revenue": number }>(
      { source_model: "orders", measures: ["revenue"], variables: { a: 1 } },
      { variables: { b: 2 }, dryRun: true },
    );
    expect(result.data[0]?.["orders.revenue"]).toBe(1);
    const [url, init] = fetch.mock.calls[0]!;
    expect(String(url)).toBe("http://gw/query");
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer t1");
    expect(JSON.parse(String(init?.body))).toEqual({
      source_model: "orders", measures: ["revenue"], variables: { a: 1, b: 2 }, dry_run: true,
    });
  });

  it("withToken swaps the token only", async () => {
    const fetch = mockFetch(200, []);
    const base = new SlayerClient({ baseUrl: "http://gw", token: "service", fetch });
    await base.withToken("user").listModels("shared");
    const [url, init] = fetch.mock.calls[0]!;
    expect(String(url)).toBe("http://gw/models?data_source=shared");
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer user");
  });

  it("throws SlayerApiError with the detail", async () => {
    const client = new SlayerClient({ baseUrl: "http://gw", fetch: mockFetch(403, { detail: "no" }) });
    const error = await client.health().catch((e: unknown) => e);
    expect(error).toBeInstanceOf(SlayerApiError);
    expect((error as SlayerApiError).isForbidden).toBe(true);
    expect((error as SlayerApiError).detail).toBe("no");
  });
});

describe("signGatewayToken", () => {
  it("signs a verifiable HS256 token", () => {
    const token = signGatewayToken({ sub: "u", tenant: "acme", roles: ["analyst"] }, "s".repeat(32), 60);
    const [, payload] = token.split(".");
    const claims = JSON.parse(Buffer.from(payload!, "base64url").toString());
    expect(claims).toMatchObject({ sub: "u", tenant: "acme", roles: ["analyst"] });
    expect(claims.exp - claims.iat).toBe(60);
  });

  it("rejects short secrets", () => {
    expect(() => signGatewayToken({ sub: "u", tenant: "t", roles: [] }, "short")).toThrow();
  });
});
