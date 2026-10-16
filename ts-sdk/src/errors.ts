/** A non-2xx response from SLayer or the gateway. */
export class SlayerApiError extends Error {
  readonly status: number;
  readonly detail: unknown;

  constructor(status: number, detail: unknown) {
    super(`SLayer API error ${status}: ${typeof detail === "string" ? detail : JSON.stringify(detail)}`);
    this.name = "SlayerApiError";
    this.status = status;
    this.detail = detail;
  }

  /** The token is missing, invalid or expired. */
  get isUnauthorized(): boolean {
    return this.status === 401;
  }

  /** The role may not do this. */
  get isForbidden(): boolean {
    return this.status === 403;
  }
}
