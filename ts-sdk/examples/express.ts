// A Node backend that forwards each user's own token to the gateway, so SLayer applies that user's tenant and role.
// Run it with your web framework of choice; this sketch uses Express-style handlers.
import { SlayerApiError, SlayerClient } from "@motley-ai/slayer";

const slayer = new SlayerClient({ baseUrl: process.env.SLAYER_URL ?? "http://slayer-gateway:5143", timeoutMs: 30_000 });

interface Req {
  headers: Record<string, string | undefined>;
  query: Record<string, string | undefined>;
}
interface Res {
  status(code: number): Res;
  json(body: unknown): void;
}

export async function revenueByCustomer(req: Req, res: Res): Promise<void> {
  const token = req.headers.authorization?.replace(/^Bearer /, "");
  if (!token) return res.status(401).json({ error: "unauthorized" });
  try {
    const result = await slayer.withToken(token).query<{ "orders.customers.name": string; "orders.revenue": number }>({
      source_model: "orders",
      dimensions: ["customers.name"],
      measures: ["revenue"],
      time_dimensions: [{ dimension: "created_at", granularity: "month", date_range: req.query.range ?? "last 12 months" }],
      order: [{ column: "revenue", direction: "desc" }],
    });
    res.json(result.data);
  } catch (error) {
    if (error instanceof SlayerApiError) return res.status(error.status).json({ error: error.detail });
    throw error;
  }
}
