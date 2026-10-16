import { createHmac } from "node:crypto";

export interface DevTokenClaims {
  sub: string;
  tenant: string;
  roles: string[];
  [claim: string]: unknown;
}

/**
 * Sign an HS256 token for the gateway's `auth.hs256_secret`.
 * Use it for local development and service-to-service calls. For end users, prefer tokens from your identity provider.
 */
export function signGatewayToken(claims: DevTokenClaims, secret: string, ttlSeconds = 3600): string {
  if (Buffer.byteLength(secret) < 32) throw new Error("Use a secret of at least 32 bytes");
  const now = Math.floor(Date.now() / 1000);
  const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const unsigned = `${encode({ alg: "HS256", typ: "JWT" })}.${encode({ iat: now, exp: now + ttlSeconds, ...claims })}`;
  const signature = createHmac("sha256", secret).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}
