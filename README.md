<p align="center">
  <img src="https://raw.githubusercontent.com/MotleyAI/slayer/main/docs/images/slayer-hero.png" alt="SLayer — AI agent operating a semantic layer" width="600">
</p>

[![PyPI](https://img.shields.io/pypi/v/motley-slayer?label=PyPI)](https://pypi.org/project/motley-slayer/)
[![Python](https://img.shields.io/pypi/pyversions/motley-slayer)](https://pypi.org/project/motley-slayer/)
[![Docs](https://img.shields.io/badge/docs-docs.motley.ai-blue)](https://docs.motley.ai/slayer/)
[![License](https://img.shields.io/github/license/MotleyAI/slayer)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/MotleyAI/slayer?style=social)](https://github.com/MotleyAI/slayer/stargazers)
[![Discord](https://img.shields.io/badge/Discord-join-5865F2?logo=discord&logoColor=white)](https://discord.gg/egWxMctHCA)


# SLayer

An expressive, embeddable semantic layer for AI agents and humans.

SLayer enables AI-powered data analytics on top of your warehouse. Agents get a governed, shared surface through which they access your data and metrics and give you reliable answers.

SLayer handles database connectivity (read-only), SQL translation, common data transformations, and row-level security, so LLMs and humans don't have to. Adapt it to your workflows, not the other way around. Manage definitions easily with an agent or by yourself.

SLayer can be used as a standalone tool or imported as a Python library, easily embeddable into any Python app. Use it for powering analytical MCP servers or APIs or simply to query databases semantically.

### How SLayer is different

Traditionally, semantic layers were a part of the BI stack, where every metric and its aggregation
had to be predefined. Agents need more flexibility because users ask questions that involve metric combinations (like ratios), transforms (like time shifts), or different aggregations
of the same metric (like average instead of sum).

SLayer allows to define a column `revenue` once and query it using [expressions](https://docs.motley.ai/slayer/concepts/formulas/) like `revenue:sum`, `revenue:avg`, `revenue:sum / *:count`, `time_shift(revenue:sum, -1, 'year')` etc.

SLayer is focused on the common agentic **search → inspect → query** flow. It has a [search](https://docs.motley.ai/slayer/concepts/search/) tool for efficient discovery and a [memory store](https://docs.motley.ai/slayer/concepts/memories/) for linking the relevant business context.

Agents, apps and humans can talk to SLayer via [MCP](https://docs.motley.ai/slayer/reference/mcp/), [REST API](https://docs.motley.ai/slayer/reference/rest-api/), [CLI](https://docs.motley.ai/slayer/reference/cli/), [Python](https://docs.motley.ai/slayer/reference/python-client/), [Flight SQL](https://docs.motley.ai/slayer/interfaces/flight-sql/), or Postgres-based [SQL API](https://docs.motley.ai/slayer/interfaces/pg-facade/). SLayer [supports](https://docs.motley.ai/slayer/configuration/datasources/#supported-database-types) most popular databases.

SLayer fits next to your existing data stack. It also provides importers for [dbt](https://docs.motley.ai/slayer/dbt/dbt_import/), [Cube](https://docs.motley.ai/slayer/cube/cube_import/), and [Ossie](https://docs.motley.ai/slayer/osi/osi_import/) configs.

**See [docs](https://docs.motley.ai/slayer/) for more.**

### Example

Question (run on the built-in demo Jaffle Shop database): **"show monthly revenue by store, with month-over-month % change"**

Side by side, here's LLM-generated SQL and the equivalent SLayer query.

![Example SQL vs SLayer query](https://github.com/user-attachments/assets/a8c73688-e760-402e-9f87-a05591d6cbee)

## Quickstart

We recommend using [uv](https://docs.astral.sh/uv/), especially if you don't work in a Python project.

```bash
uv tool install 'motley-slayer[all]'
```

If `slayer` isn't found on PATH afterwards, run `uv tool update-shell` and reopen your terminal.

### Using demo dataset

```bash
# With the Jaffle Shop demo preloaded (zero-config quickstart)
claude mcp add slayer_demo -- slayer mcp --demo
```

### Using your own data

Set up your datasource, substituting the correct database, username, hostname, and db_name.

```bash
slayer datasources create 'postgresql://user:${DB_PASSWORD}@hostname/db_name'
```

The password will be read by SLayer at init time, not saved to disk nor exposed to Claude.

Then add SLayer to Claude Code:

```bash
claude mcp add slayer -- slayer mcp --ingest-on-startup
```

Now SLayer MCP will be visible in Claude Code next time you start it. Make sure to launch Claude Code from a shell where `DB_PASSWORD` is exported — the MCP subprocess inherits its environment from the launching process.

Read more on how to get started with [MCP](https://docs.motley.ai/slayer/getting-started/mcp/), [CLI](https://docs.motley.ai/slayer/getting-started/cli/), [REST API](https://docs.motley.ai/slayer/getting-started/rest-api/), [Python](https://docs.motley.ai/slayer/getting-started/python/) in the docs.

## License

MIT
