"""Tunables for the engine A/B audit (branch vs pinned PyPI release)."""

PYPI_PIN = "0.9.12"

# Full-corpus scales; the subset scale runs only entries flagged subset_100k.
SCALES: dict[str, int] = {"10k": 10_000, "40k": 40_000}
SUBSET_SCALE: tuple[str, int] = ("100k", 100_000)
# Opt-in big scales (never in the default --scales): pass e.g.
# --scales 1m --entries <subset ids> for subset timing at power-of-10 sizes.
EXTRA_SCALES: dict[str, int] = {"100k": 100_000, "1m": 1_000_000, "10m": 10_000_000}

CORRECTNESS_SCALE = "10k"

REPEATS = 7           # timed repeats per query per runner invocation
PERF_RATIO = 1.3      # branch median must exceed pypi median by this factor...
PERF_FLOOR = 0.020    # ...AND by this many seconds, to be flagged

BACKENDS = ["sqlite", "duckdb"]

DATA_START_DATE = "2023-01-01"
DATA_END_DATE = "2024-12-31"
SEED = 42

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at)",
    "CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_shop_id ON orders (shop_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_category ON orders (category)",
    "CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers (segment)",
    "CREATE INDEX IF NOT EXISTS idx_shops_region_id ON shops (region_id)",
]

# Adversarial dataset DDL: same shape as tests/perf/seed.py tables but fully
# nullable, so pathological rows (null FKs, null costs) can be inserted.
ADVERSARIAL_DDL = """
CREATE TABLE IF NOT EXISTS regions (id INTEGER, name TEXT);
CREATE TABLE IF NOT EXISTS shops (
    id INTEGER, name TEXT, region_id INTEGER,
    avg_cost INTEGER, avg_frequency INTEGER, size INTEGER
);
CREATE TABLE IF NOT EXISTS customers (
    id INTEGER, name TEXT, segment TEXT, primary_shop_id INTEGER
);
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER, customer_id INTEGER, shop_id INTEGER, category TEXT,
    cost INTEGER, created_at TIMESTAMP, completed_at TIMESTAMP, cancelled_at TIMESTAMP
);
"""
