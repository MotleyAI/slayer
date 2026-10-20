import { createHmac } from "node:crypto";

export interface DevTokenClaims {
  sub: string;
  tenant: string;
  roles: string[];
  [claim: string]: unknown;
}

export interface SignGatewayTokenOptions {
  /** Defaults to 3600. */
  ttlSeconds?: number;
  /** Issue time in Unix seconds; defaults to now. */
  now?: number;
}

/**
 * Sign an HS256 token for the gateway's `auth.hs256_secret`; byte-identical to Python's `sign_gateway_token`.
 * Use it for local development and service-to-service calls. For end users, prefer tokens from your identity provider.
 */
export function signGatewayToken(claims: DevTokenClaims, secret: string, options: SignGatewayTokenOptions = {}): string {
  if (Buffer.byteLength(secret) < 32) throw new Error("Use a secret of at least 32 bytes");
  const ttlSeconds = options.ttlSeconds ?? 3600;
  const now = options.now ?? Math.floor(Date.now() / 1000);
  const encode = (value: object) => Buffer.from(JSON.stringify(value)).toString("base64url");
  const unsigned = `${encode({ alg: "HS256", typ: "JWT" })}.${encode({ iat: now, exp: now + ttlSeconds, ...claims })}`;
  const signature = createHmac("sha256", secret).update(unsigned).digest("base64url");
  return `${unsigned}.${signature}`;
}
