import { readdirSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const src = new URL("../src/", import.meta.url);
const read = (name: string) => readFileSync(new URL(name, src), "utf8");

describe("portable entry", () => {
  it("imports no node: module outside src/node.ts", () => {
    const offenders = readdirSync(src)
      .filter((name) => name.endsWith(".ts") && name !== "node.ts")
      .filter((name) => /(from\s+|import\s*\(\s*)["']node:/.test(read(name)));
    expect(offenders).toEqual([]);
  });

  it("declares no runtime dependencies", () => {
    const manifest = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8")) as {
      dependencies?: Record<string, string>;
    };
    expect(Object.keys(manifest.dependencies ?? {})).toEqual([]);
  });
});
