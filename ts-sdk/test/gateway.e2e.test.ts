// Runs against the slayer-custom demo stack: SLAYER_E2E_URL=http://localhost:5143 npm test
import { describe, expect, it } from "vitest";
import { SlayerClient } from "../src/index.js";
import { signGatewayToken } from "../src/node.js";

const url = process.env.SLAYER_E2E_URL;
const secret = process.env.GATEWAY_JWT_SECRET ?? "demo-secret-change-me-at-least-32-bytes";

describe.skipIf(!url)("gateway demo", () => {
  const client = new SlayerClient({ url: url ?? "", timeoutMs: 10_000 });
  const as = (tenant: string, role: string) =>
    client.withToken(signGatewayToken({ sub: "e2e", tenant, roles: [role] }, secret));

  it("scopes rows to the tenant", async () => {
    const query = { source_model: "orders", measures: [{ formula: "revenue", name: "revenue" }] };
    const acme = await as("acme", "analyst").query<{ "orders.revenue": number }>(query);
    const all = await as("acme", "platform_admin").query<{ "orders.revenue": number }>(query);
    expect(acme.data[0]?.["orders.revenue"]).toBe(1800);
    expect(all.data[0]?.["orders.revenue"]).toBe(10800);
  });

  it("hides columns and models by role", async () => {
    const analyst = as("acme", "analyst");
    await expect(analyst.query({ source_model: "customers", dimensions: ["email"] })).rejects.toThrow();
    const names = (await analyst.listModels()).map((m) => m.name);
    expect(names).not.toContain("employees");
  });

  it("reads per-tenant schemas", async () => {
    const result = await as("globex", "admin").query({ source_model: "employees", dimensions: ["name"] });
    expect(result.data).toEqual([{ "employees.name": "Cid" }]);
  });

  it("rejects writes without permission", async () => {
    await expect(as("acme", "admin").deleteModel("orders")).rejects.toMatchObject({ status: 403 });
  });
});
