"""Benchmark parameters — single source of truth for all perf test settings.

Adjust these to change benchmark behavior without touching test logic.
"""

# ---------------------------------------------------------------------------
# Database backend
# ---------------------------------------------------------------------------

# "sqlite" — auto-created temp file, zero setup (default)
# "url" — connect to an external DB (Docker, remote, any SQLAlchemy-supported dialect)
DB_BACKEND = "sqlite"

# Settings for DB_BACKEND = "url"
# DB_URL = "postgresql://user:pass@localhost:5432/slayer_bench"
# DB_TYPE = "postgres"  # SLayer datasource type: postgres, mysql, clickhouse, etc.
DB_URL = ""
DB_TYPE = ""

# ---------------------------------------------------------------------------
# Data scales — each entry generates a fixture and a test class
# ---------------------------------------------------------------------------

SCALES: dict[str, int] = {
    "1k": 1_000,
    "10k": 10_000,
    "40k": 40_000,
    # "100k": 100_000,
    # "200k": 200_000,
    # "400k": 400_000,
}

# ---------------------------------------------------------------------------
# Date ranges
# ---------------------------------------------------------------------------

# Full range of generated data
DATA_START_DATE = "2023-01-01"
DATA_END_DATE = "2024-12-31"

# Typical query date range (subset of data range, ~7 months)
QUERY_DATE_RANGE = ["2024-06-01", "2024-12-31"]

# ---------------------------------------------------------------------------
# Database indexes — applied after seeding for realistic query performance
# ---------------------------------------------------------------------------

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_shop_id ON orders (shop_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_category ON orders (category)",
    "CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers (segment)",
    "CREATE INDEX IF NOT EXISTS idx_customers_primary_shop ON customers (primary_shop_id)",
    "CREATE INDEX IF NOT EXISTS idx_shops_region_id ON shops (region_id)",
]

# ---------------------------------------------------------------------------
# Seed settings
# ---------------------------------------------------------------------------

SEED = 42  # LCG base seed for reproducibility
