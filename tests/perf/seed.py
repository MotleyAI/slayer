"""Deterministic pseudo-random e-commerce data generator for performance testing.

Uses integer arithmetic with prime-based LCG for reproducibility.
No external dependencies — pure Python math.

Usage:
    from tests.perf.seed import generate_dataset, seed_database

    dataset = generate_dataset(order_count=10_000)
    seed_database(engine, dataset)  # SQLAlchemy engine
"""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# LCG — Linear Congruential Generator (Knuth's constants)
# ---------------------------------------------------------------------------

_LCG_A = 6364136223846793005
_LCG_C = 1442695040888963407
_LCG_M = 2**64


def _prng(seed: int) -> int:
    """One step of the LCG. Returns the next pseudo-random value."""
    return (_LCG_A * seed + _LCG_C) % _LCG_M


def _prng_float(seed: int) -> float:
    """Return a float in [0, 1) from a seed."""
    return (seed % (2**32)) / (2**32)


def _prng_range(seed: int, lo: int, hi: int) -> int:
    """Return an int in [lo, hi] from a seed."""
    return lo + (seed % (hi - lo + 1))


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

REGIONS = [
    "North America", "Europe", "Asia Pacific", "Latin America",
    "Middle East", "Africa", "Oceania",
]

SHOP_NAME_PREFIXES = [
    "Prime", "Star", "Metro", "Grand", "Nova", "Atlas", "Apex",
    "Peak", "Edge", "Core", "Zen", "Bolt", "Flux", "Pulse", "Vertex",
]
SHOP_NAME_SUFFIXES = [
    "Market", "Store", "Shop", "Mall", "Depot", "Hub", "Plaza",
    "Outlet", "Bazaar", "Emporium",
]

FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace",
    "Henry", "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia",
    "Pete", "Quinn", "Rosa", "Sam", "Tina", "Uma", "Vic", "Wendy",
    "Xander", "Yara", "Zack",
]
LAST_NAMES = [
    "Smith", "Jones", "Lee", "Chen", "Garcia", "Kim", "Patel",
    "Brown", "Silva", "Müller", "Tanaka", "Ali", "Lopez", "Wang",
    "Martin", "Anderson", "Taylor", "Thomas", "Wilson", "Moore",
]

ORDER_CATEGORIES = ["electronics", "clothing", "food", "home", "sports"]

# Segment definitions: (name, cost_multiplier_pct, frequency_multiplier_pct, cumulative_weight)
# cumulative_weight out of 100: poor=15, average=70, upper=12, whale=3
SEGMENTS = [
    ("poor", 60, 70, 15),       # -40% cost, -30% frequency
    ("average", 100, 100, 85),  # baseline
    ("upper", 130, 120, 97),    # +30% cost, +20% frequency
    ("whale", 200, 120, 100),   # +100% cost, +20% frequency
]

# Seasonality lookup tables (multiplied to get combined weight)
MONTHLY_WEIGHTS = [85, 80, 90, 95, 100, 105, 100, 95, 110, 105, 120, 130]
WEEKDAY_WEIGHTS = [90, 95, 100, 100, 105, 115, 110]  # Mon=0 .. Sun=6


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Region:
    id: int
    name: str


@dataclass
class Shop:
    id: int
    name: str
    region_id: int
    avg_cost: int         # in cents, avoids float
    avg_frequency: int    # relative weight for order frequency
    size: int             # relative weight for customer assignment


@dataclass
class Customer:
    id: int
    name: str
    segment: str
    shop_ids: list[int]   # primary shop first, then optional secondary/tertiary


@dataclass
class Order:
    id: int
    customer_id: int
    shop_id: int
    category: str
    cost: int             # in cents
    created_at: datetime
    completed_at: datetime | None
    cancelled_at: datetime | None


@dataclass
class Dataset:
    regions: list[Region]
    shops: list[Shop]
    customers: list[Customer]
    orders: list[Order]


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

def generate_dataset(
    order_count: int,
    start_date: str = "2023-01-01",
    end_date: str = "2024-12-31",
    seed: int = 42,
) -> Dataset:
    """Generate a deterministic e-commerce dataset.

    Args:
        order_count: Target number of orders.
        start_date: First possible order date (YYYY-MM-DD).
        end_date: Last possible order date (YYYY-MM-DD).
        seed: Base seed for the LCG.
    """
    if order_count < 0:
        raise ValueError(f"order_count must be non-negative, got {order_count}")
    dt_start = datetime.strptime(start_date, "%Y-%m-%d")
    dt_end = datetime.strptime(end_date, "%Y-%m-%d")
    if dt_start > dt_end:
        raise ValueError(f"start_date ({start_date}) must be <= end_date ({end_date})")

    s = _prng(seed)

    region_count = 2 + round(math.log(max(order_count, 2), 30))
    shop_count = 4 + round(math.log(max(order_count, 2), 15))
    customer_count = 100 + 100 * round(math.log(max(order_count, 10), 10))

    # --- Regions ---
    regions = []
    for i in range(region_count):
        regions.append(Region(id=i + 1, name=REGIONS[i % len(REGIONS)]))

    # --- Shops ---
    shops = []
    for i in range(shop_count):
        s = _prng(s)
        prefix = SHOP_NAME_PREFIXES[s % len(SHOP_NAME_PREFIXES)]
        s = _prng(s)
        suffix = SHOP_NAME_SUFFIXES[s % len(SHOP_NAME_SUFFIXES)]
        name = f"{prefix} {suffix}"

        s = _prng(s)
        region_id = (s % region_count) + 1
        s = _prng(s)
        avg_cost = _prng_range(s, 1500, 8000)  # $15 - $80
        s = _prng(s)
        avg_frequency = _prng_range(s, 50, 200)
        s = _prng(s)
        size = _prng_range(s, 30, 200)

        shops.append(Shop(
            id=i + 1, name=name, region_id=region_id,
            avg_cost=avg_cost, avg_frequency=avg_frequency, size=size,
        ))

    # --- Customers ---
    # Weighted assignment: pick primary shop by shop.size
    shop_size_cumsum = []
    total_size = 0
    for shop in shops:
        total_size += shop.size
        shop_size_cumsum.append(total_size)

    customers = []
    for i in range(customer_count):
        s = _prng(s)
        # Segment selection
        seg_roll = s % 100
        segment = "average"
        for seg_name, _, _, cum_weight in SEGMENTS:
            if seg_roll < cum_weight:
                segment = seg_name
                break

        # Primary shop (weighted by size)
        s = _prng(s)
        roll = s % total_size
        primary_shop_id = shops[-1].id
        for j, cum in enumerate(shop_size_cumsum):
            if roll < cum:
                primary_shop_id = shops[j].id
                break

        shop_ids = [primary_shop_id]

        # 1/5 chance of secondary shop
        s = _prng(s)
        if s % 5 == 0:
            s = _prng(s)
            secondary = (s % shop_count) + 1
            if secondary != primary_shop_id:
                shop_ids.append(secondary)

        # 1/50 chance of tertiary shop
        s = _prng(s)
        if s % 50 == 0:
            s = _prng(s)
            tertiary = (s % shop_count) + 1
            if tertiary not in shop_ids:
                shop_ids.append(tertiary)

        first = FIRST_NAMES[i % len(FIRST_NAMES)]
        last = LAST_NAMES[(i * 7 + 3) % len(LAST_NAMES)]
        name = f"{first} {last}"

        customers.append(Customer(
            id=i + 1, name=name, segment=segment, shop_ids=shop_ids,
        ))

    # --- Pre-compute day weights for date distribution ---
    total_days = (dt_end - dt_start).days + 1
    day_weights = []
    day_cumsum = []
    cum = 0
    for d in range(total_days):
        dt = dt_start + timedelta(days=d)
        mw = MONTHLY_WEIGHTS[dt.month - 1]
        dw = WEEKDAY_WEIGHTS[dt.weekday()]
        w = mw * dw
        day_weights.append(w)
        cum += w
        day_cumsum.append(cum)
    total_day_weight = cum

    # --- Pre-compute customer-shop pair weights ---
    # Each pair's weight = shop.avg_frequency * segment.frequency_multiplier
    segment_freq = {seg[0]: seg[2] for seg in SEGMENTS}
    pairs = []       # (customer_id, shop_id)
    pair_weights = []
    for cust in customers:
        freq_mult = segment_freq[cust.segment]
        for idx, shop_id in enumerate(cust.shop_ids):
            shop = shops[shop_id - 1]
            # Secondary/tertiary shops visited less often
            visit_factor = 100 if idx == 0 else 30 if idx == 1 else 10
            weight = shop.avg_frequency * freq_mult * visit_factor // 100
            pairs.append((cust.id, shop_id))
            pair_weights.append(weight)

    pair_cumsum = []
    cum = 0
    for w in pair_weights:
        cum += w
        pair_cumsum.append(cum)
    total_pair_weight = cum

    # --- Segment cost multipliers ---
    segment_cost = {seg[0]: seg[1] for seg in SEGMENTS}

    # --- Generate orders ---
    # Some shops open later (shop.id > shop_count * 0.7 opens halfway through)
    mid_date = dt_start + (dt_end - dt_start) // 2
    late_shop_threshold = max(1, int(shop_count * 0.7))

    orders = []
    for i in range(order_count):
        order_seed = _prng(seed + i * 982451653)  # prime stride per order

        # Pick customer-shop pair (weighted)
        s2 = _prng(order_seed)
        roll = s2 % total_pair_weight
        pair_idx = _bisect(pair_cumsum, roll)
        cust_id, shop_id = pairs[pair_idx]

        # Pick day (weighted by seasonality)
        s2 = _prng(s2)
        day_roll = s2 % total_day_weight
        day_idx = _bisect(day_cumsum, day_roll)

        # Hour within day (8am - 10pm)
        s2 = _prng(s2)
        hour = _prng_range(s2, 8, 22)
        s2 = _prng(s2)
        minute = s2 % 60

        created_at = dt_start + timedelta(days=day_idx, hours=hour, minutes=minute)

        # Late-opening shops: if order is before mid_date, re-assign to an earlier shop
        if shop_id > late_shop_threshold and created_at < mid_date:
            shop_id = (shop_id % late_shop_threshold) + 1

        # Cost: shop avg * segment multiplier * random ±20%
        shop = shops[shop_id - 1]
        cust = customers[cust_id - 1]
        cost_mult = segment_cost[cust.segment]
        s2 = _prng(s2)
        jitter = _prng_range(s2, 80, 120)  # ±20%
        cost = shop.avg_cost * cost_mult * jitter // 10000

        # Category
        s2 = _prng(s2)
        category = ORDER_CATEGORIES[s2 % len(ORDER_CATEGORIES)]

        # Cancellation: 5% cancelled
        s2 = _prng(s2)
        cancelled = (s2 % 100) < 5
        cancelled_at = None
        completed_at = None

        if cancelled:
            s2 = _prng(s2)
            cancel_hours = _prng_range(s2, 1, 72)
            cancelled_at = created_at + timedelta(hours=cancel_hours)
        else:
            # 99% of non-cancelled orders are completed
            s2 = _prng(s2)
            if (s2 % 100) < 99:
                s2 = _prng(s2)
                complete_hours = _prng_range(s2, 1, 168)  # 1h - 7 days
                completed_at = created_at + timedelta(hours=complete_hours)

        orders.append(Order(
            id=i + 1, customer_id=cust_id, shop_id=shop_id,
            category=category, cost=cost,
            created_at=created_at, completed_at=completed_at,
            cancelled_at=cancelled_at,
        ))

    return Dataset(regions=regions, shops=shops, customers=customers, orders=orders)


def _bisect(cumsum: list[int], value: int) -> int:
    """Binary search in cumulative sum array. Returns index where value falls."""
    lo, hi = 0, len(cumsum) - 1
    while lo < hi:
        mid = (lo + hi) // 2
        if cumsum[mid] <= value:
            lo = mid + 1
        else:
            hi = mid
    return lo


# ---------------------------------------------------------------------------
# Database seeding (SQLAlchemy-agnostic)
# ---------------------------------------------------------------------------

_DROP_TABLES_SQL = """
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS shops;
DROP TABLE IF EXISTS regions;
"""

_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS regions (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS shops (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    region_id INTEGER NOT NULL,
    avg_cost INTEGER NOT NULL,
    avg_frequency INTEGER NOT NULL,
    size INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    segment TEXT NOT NULL,
    primary_shop_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    shop_id INTEGER NOT NULL,
    category TEXT NOT NULL,
    cost INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    cancelled_at TIMESTAMP
);
"""


def _fmt_dt(dt: datetime | None, use_iso: bool) -> str | datetime | None:
    """Format a datetime for insertion — isoformat for SQLite, native for others."""
    if dt is None:
        return None
    return dt.isoformat() if use_iso else dt


def seed_database(engine, dataset: Dataset, clean: bool = False) -> None:
    """Seed a database via SQLAlchemy engine. Works with any supported dialect.

    Args:
        clean: If True, drop and recreate tables before seeding.
            Use for external DBs that persist between runs.
    """
    import sqlalchemy as sa

    with engine.connect() as conn:
        if clean:
            for stmt in _DROP_TABLES_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(sa.text(stmt))
            conn.commit()

        for stmt in _CREATE_TABLES_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(sa.text(stmt))

        # Regions
        for r in dataset.regions:
            conn.execute(sa.text(
                "INSERT INTO regions (id, name) VALUES (:id, :name)"
            ), {"id": r.id, "name": r.name})

        # Shops
        for s in dataset.shops:
            conn.execute(sa.text(
                "INSERT INTO shops (id, name, region_id, avg_cost, avg_frequency, size) "
                "VALUES (:id, :name, :region_id, :avg_cost, :avg_frequency, :size)"
            ), {"id": s.id, "name": s.name, "region_id": s.region_id,
                "avg_cost": s.avg_cost, "avg_frequency": s.avg_frequency, "size": s.size})

        # Customers
        for c in dataset.customers:
            conn.execute(sa.text(
                "INSERT INTO customers (id, name, segment, primary_shop_id) "
                "VALUES (:id, :name, :segment, :primary_shop_id)"
            ), {"id": c.id, "name": c.name, "segment": c.segment,
                "primary_shop_id": c.shop_ids[0]})

        # Orders (batch insert for performance)
        # SQLite needs isoformat strings; other DBs use native datetime
        use_iso = engine.dialect.name == "sqlite"
        batch_size = 1000
        for i in range(0, len(dataset.orders), batch_size):
            batch = dataset.orders[i:i + batch_size]
            conn.execute(
                sa.text(
                    "INSERT INTO orders (id, customer_id, shop_id, category, cost, "
                    "created_at, completed_at, cancelled_at) "
                    "VALUES (:id, :customer_id, :shop_id, :category, :cost, "
                    ":created_at, :completed_at, :cancelled_at)"
                ),
                [{"id": o.id, "customer_id": o.customer_id, "shop_id": o.shop_id,
                  "category": o.category, "cost": o.cost,
                  "created_at": _fmt_dt(o.created_at, use_iso),
                  "completed_at": _fmt_dt(o.completed_at, use_iso),
                  "cancelled_at": _fmt_dt(o.cancelled_at, use_iso),
                  } for o in batch],
            )

        conn.commit()
