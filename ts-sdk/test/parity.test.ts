// TS half of the client parity harness: replays tests/client_parity.json (Python half: tests/test_client_parity.py).
import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { SlayerClient } from "../src/index.js";
import { signGatewayToken } from "../src/node.js";

interface Expectation {
  method: string;
  path: string;
  query: Record<string, string>;
  body: unknown;
  headers?: Record<string, string>;
  absent_headers?: string[];
}

interface Case {
  name: string;
  op: string;
  client?: { token?: string; headers?: Record<string, string> };
  with_token?: string;
  ts: { args: unknown[] };
  expect: Expectation;
  returns?: unknown;
}

interface SigningCase {
  name: string;
  claims: { sub: string; tenant: string; roles: string[] };
  secret: string;
  ts: { options: Record<string, number> };
  expected?: string;
  error?: boolean;
}

interface Manifest {
  base_url: string;
  operations: { python: string; ts: string }[];
  ts_only: string[];
  responses: Record<string, unknown>;
  cases: Case[];
  signing: SigningCase[];
}

const manifest = JSON.parse(
  readFileSync(new URL("../../tests/client_parity.json", import.meta.url), "utf8"),
) as Manifest;
const tsName = new Map(manifest.operations.map((op) => [op.python, op.ts]));

type Method = (...args: unknown[]) => unknown;

describe("client parity", () => {
  it("exposes exactly the manifest's operations", () => {
    // Internals must be #private so the prototype lists only the public surface.
    const methods = Object.getOwnPropertyNames(SlayerClient.prototype).filter((name) => name !== "constructor");
    const expected = [...manifest.operations.map((op) => op.ts), ...manifest.ts_only];
    expect(methods.sort()).toEqual(expected.sort());
  });

  it.each(manifest.cases.map((c) => [c.name, c] as const))("%s", async (_name, c) => {
    const fetch = vi.fn(async (_url: URL | RequestInfo, _init?: RequestInit) =>
      new Response(JSON.stringify(manifest.responses[c.op]), { status: 200, headers: { "Content-Type": "application/json" } }),
    );
    let client = new SlayerClient({ url: manifest.base_url, ...c.client, fetch });
    if (c.with_token !== undefined) client = client.withToken(c.with_token);

    const method = (client as unknown as Record<string, Method>)[tsName.get(c.op)!]!;
    const result = await method.apply(client, c.ts.args);

    expect(fetch).toHaveBeenCalledTimes(1);
    const [rawUrl, init] = fetch.mock.calls[0]!;
    const url = new URL(String(rawUrl));
    const headers = new Headers(init?.headers);
    expect([init?.method ?? "GET", url.pathname, Object.fromEntries(url.searchParams)]).toEqual([
      c.expect.method, c.expect.path, c.expect.query,
    ]);
    expect(init?.body === undefined ? null : JSON.parse(String(init.body))).toEqual(c.expect.body);
    for (const [name, value] of Object.entries(c.expect.headers ?? {})) expect(headers.get(name)).toBe(value);
    for (const name of c.expect.absent_headers ?? []) expect(headers.has(name)).toBe(false);
    if (c.returns !== undefined) expect(result).toEqual(c.returns);
  });

  it.each(manifest.signing.map((c) => [c.name, c] as const))("signing: %s", (_name, c) => {
    const sign = () => signGatewayToken(c.claims, c.secret, c.ts.options);
    if (c.error) expect(sign).toThrow();
    else expect(sign()).toBe(c.expected);
  });
});
