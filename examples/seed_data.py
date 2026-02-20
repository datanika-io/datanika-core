"""
Seed script for the Online Store example dataset.

Populates MySQL, MSSQL, and MongoDB with a coherent dataset:
  - MongoDB: 500 users
  - MySQL: 50 sellers, 500 goods, 2000 orders, ~5000 order items
  - MSSQL: 3000 ratings, 1000 reviews
"""

import argparse
import configparser
import json
import logging
import os
import random
import string
import time
from datetime import datetime, timedelta

log = logging.getLogger("seed_data")

# ---------------------------------------------------------------------------
# Config file loading
# ---------------------------------------------------------------------------

def load_conf():
    """Load databases.conf from the same directory as this script."""
    conf = configparser.ConfigParser()
    conf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "databases.conf")
    conf.read(conf_path)
    return conf


# ---------------------------------------------------------------------------
# CLI arguments
# ---------------------------------------------------------------------------

def parse_args():
    conf = load_conf()
    p = argparse.ArgumentParser(description="Seed the online store example databases")
    p.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", conf.get("mysql", "host", fallback="localhost")))
    p.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", conf.get("mysql", "port", fallback="3306"))))
    p.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", conf.get("mysql", "user", fallback="root")))
    p.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", conf.get("mysql", "password", fallback="root")))
    p.add_argument("--mysql-db", default=os.getenv("MYSQL_DB", conf.get("mysql", "database", fallback="online_store")))
    p.add_argument("--mssql-host", default=os.getenv("MSSQL_HOST", conf.get("mssql", "host", fallback="localhost")))
    p.add_argument("--mssql-port", type=int, default=int(os.getenv("MSSQL_PORT", conf.get("mssql", "port", fallback="1433"))))
    p.add_argument("--mssql-user", default=os.getenv("MSSQL_USER", conf.get("mssql", "user", fallback="sa")))
    p.add_argument("--mssql-password", default=os.getenv("MSSQL_PASSWORD", conf.get("mssql", "password", fallback="SA_Password1!")))
    p.add_argument("--mssql-db", default=os.getenv("MSSQL_DB", conf.get("mssql", "database", fallback="online_store")))
    p.add_argument("--mongo-host", default=os.getenv("MONGO_HOST", conf.get("mongodb", "host", fallback="localhost")))
    p.add_argument("--mongo-port", type=int, default=int(os.getenv("MONGO_PORT", conf.get("mongodb", "port", fallback="27017"))))
    p.add_argument("--mongo-db", default=os.getenv("MONGO_DB", conf.get("mongodb", "database", fallback="online_store")))
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    p.add_argument("--multiplier", type=float, default=float(conf.get("seed", "multiplier", fallback="1")), help="Scale all record counts by this factor")
    p.add_argument("--seed", type=int, default=int(conf.get("seed", "seed", fallback="42")), help="Random seed for reproducibility")
    p.add_argument("--users", type=int, default=int(conf.get("seed", "users", fallback="500")), help="Number of users (MongoDB)")
    p.add_argument("--sellers", type=int, default=int(conf.get("seed", "sellers", fallback="50")), help="Number of sellers")
    p.add_argument("--goods", type=int, default=int(conf.get("seed", "goods", fallback="500")), help="Number of goods")
    p.add_argument("--orders", type=int, default=int(conf.get("seed", "orders", fallback="2000")), help="Number of orders")
    p.add_argument("--order-items", type=int, default=int(conf.get("seed", "order_items", fallback="5000")), help="Target number of order items")
    p.add_argument("--ratings", type=int, default=int(conf.get("seed", "ratings", fallback="3000")), help="Number of ratings (MSSQL)")
    p.add_argument("--reviews", type=int, default=int(conf.get("seed", "reviews", fallback="1000")), help="Number of reviews (MSSQL)")
    args = p.parse_args()

    # Apply multiplier to all record counts
    if args.multiplier != 1:
        args.users = max(1, int(args.users * args.multiplier))
        args.sellers = max(1, int(args.sellers * args.multiplier))
        args.goods = max(1, int(args.goods * args.multiplier))
        args.orders = max(1, int(args.orders * args.multiplier))
        args.order_items = max(1, int(args.order_items * args.multiplier))
        args.ratings = max(1, int(args.ratings * args.multiplier))
        args.reviews = max(1, int(args.reviews * args.multiplier))

    return args


# ---------------------------------------------------------------------------
# Connection helpers with retry
# ---------------------------------------------------------------------------

CONNECT_TIMEOUT = 30  # seconds


def connect_mysql(host, port, user, password, db):
    import pymysql

    deadline = time.time() + CONNECT_TIMEOUT
    while True:
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user, password=password, database=db,
                charset="utf8mb4", autocommit=True,
            )
            log.info("MySQL connected (%s:%s)", host, port)
            return conn
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError(f"MySQL connection timed out: {exc}") from exc
            time.sleep(1)


def connect_mssql(host, port, user, password):
    import pymssql

    deadline = time.time() + CONNECT_TIMEOUT
    while True:
        try:
            conn = pymssql.connect(
                server=host, port=port, user=user, password=password,
                autocommit=True,
            )
            log.info("MSSQL connected (%s:%s)", host, port)
            return conn
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError(f"MSSQL connection timed out: {exc}") from exc
            time.sleep(1)


def connect_mongo(host, port, db):
    from pymongo import MongoClient

    deadline = time.time() + CONNECT_TIMEOUT
    while True:
        try:
            client = MongoClient(host, port, serverSelectionTimeoutMS=2000)
            client.admin.command("ping")
            log.info("MongoDB connected (%s:%s)", host, port)
            return client[db]
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError(f"MongoDB connection timed out: {exc}") from exc
            time.sleep(1)


# ---------------------------------------------------------------------------
# MSSQL: create database and schema
# ---------------------------------------------------------------------------

def setup_mssql_schema(conn, db_name, schema_file):
    cur = conn.cursor()
    cur.execute(
        f"IF DB_ID('{db_name}') IS NULL CREATE DATABASE [{db_name}]"
    )
    cur.execute(f"USE [{db_name}]")

    with open(schema_file) as f:
        sql = f.read()

    # Split on GO or semicolons and execute each statement
    for statement in sql.split(";"):
        statement = statement.strip()
        if statement:
            try:
                cur.execute(statement)
            except Exception as exc:
                log.debug("Skipping statement (already exists): %s", exc)
    cur.close()
    log.info("MSSQL schema applied to [%s]", db_name)


# ---------------------------------------------------------------------------
# Load constants from seed_constants.json
# ---------------------------------------------------------------------------

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(_SCRIPT_DIR, "seed_constants.json")) as _f:
    _C = json.load(_f)

FIRST_NAMES = _C["first_names"]
LAST_NAMES = _C["last_names"]
CITIES = _C["cities"]
STREETS = _C["streets"]
SELLER_NAMES = _C["seller_names"]
COUNTRIES = _C["countries"]
CATEGORIES = _C["categories"]
PRODUCT_ADJECTIVES = _C["product_adjectives"]
PRODUCT_NOUNS = _C["product_nouns"]
EMAIL_DOMAINS = _C["email_domains"]
REVIEW_TITLES_GOOD = _C["review_titles_good"]
REVIEW_TITLES_BAD = _C["review_titles_bad"]
REVIEW_BODIES_GOOD = _C["review_bodies_good"]
REVIEW_BODIES_BAD = _C["review_bodies_bad"]

# Expand weighted maps into flat lists for random.choice()
ORDER_STATUSES = [s for s, w in _C["order_statuses"].items() for _ in range(w)]
RATING_DISTRIBUTION = [int(r) for r, w in _C["rating_distribution"].items() for _ in range(w)]


# ---------------------------------------------------------------------------
# Data generators — try Cython-compiled version first, fall back to pure Python
# ---------------------------------------------------------------------------

# Time range: 18 months ending "now" (fixed for reproducibility)
NOW = datetime(2025, 6, 15, 12, 0, 0)
START = NOW - timedelta(days=18 * 30)

_USE_CYTHON = False
try:
    from _seed_generators import (
        generate_users,
        generate_sellers,
        generate_goods,
        generate_orders_and_items,
        generate_ratings,
        generate_reviews,
    )
    _USE_CYTHON = True
    log.debug("Using Cython-optimized generators")
except ImportError:
    log.debug("Cython generators not available, using pure Python")

if not _USE_CYTHON:

    def random_phone():
        return f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

    def random_email(first, last):
        sep = random.choice([".", "_", ""])
        suffix = random.randint(1, 999) if random.random() < 0.4 else ""
        return f"{first.lower()}{sep}{last.lower()}{suffix}@{random.choice(EMAIL_DOMAINS)}"

    def random_datetime_between(start: datetime, end: datetime) -> datetime:
        delta = (end - start).total_seconds()
        return start + timedelta(seconds=random.random() * delta)

    def generate_users(n=500):
        users = []
        for uid in range(1, n + 1):
            first = random.choice(FIRST_NAMES)
            last = random.choice(LAST_NAMES)
            created = random_datetime_between(START, NOW - timedelta(days=30))
            # Active users have recent last_login
            if random.random() < 0.6:
                last_login = random_datetime_between(NOW - timedelta(days=7), NOW)
            else:
                last_login = random_datetime_between(created, NOW)
            users.append({
                "user_id": uid,
                "first_name": first,
                "last_name": last,
                "created_at": created,
                "last_login": last_login,
                "contacts": {
                    "email": random_email(first, last),
                    "phone_number": random_phone(),
                },
                "address": {
                    "city": random.choice(CITIES),
                    "street": random.choice(STREETS),
                    "building": str(random.randint(1, 200)),
                },
            })
        return users

    def _random_suffix():
        """Generate 1-3 random uppercase letters, e.g. 'GRD', 'DF', 'A'."""
        return "".join(random.choices(string.ascii_uppercase, k=random.randint(1, 3)))

    def generate_sellers(n=50):
        sellers = []
        used_names = set()
        countries = list(set(COUNTRIES))  # deduplicate weighted list
        for sid in range(1, n + 1):
            base = random.choice(SELLER_NAMES)
            country = random.choice(countries)
            # First occurrence of a base name keeps it bare
            if base not in used_names:
                name = base
            else:
                # Append random letter suffix + country until unique
                for _ in range(1000):
                    name = f"{base} {_random_suffix()} {country}"
                    if name not in used_names:
                        break
            used_names.add(name)
            sellers.append({
                "id": sid,
                "name": name,
                "registered_at": random_datetime_between(START, NOW - timedelta(days=60)),
                "country": country,
            })
        return sellers

    def generate_goods(sellers, n=500):
        # Pareto-like: top 20% of sellers get ~60% of goods
        top_count = max(1, len(sellers) // 5)
        top_seller_ids = [s["id"] for s in sellers[:top_count]]
        other_seller_ids = [s["id"] for s in sellers[top_count:]]

        goods = []
        for gid in range(1, n + 1):
            if random.random() < 0.6:
                seller_id = random.choice(top_seller_ids)
            else:
                seller_id = random.choice(other_seller_ids)

            category = random.choice(CATEGORIES)
            noun = random.choice(PRODUCT_NOUNS[category])
            adj = random.choice(PRODUCT_ADJECTIVES)
            name = f"{adj} {noun}"
            price = round(random.uniform(5.0, 500.0), 2)
            created_at = random_datetime_between(START, NOW - timedelta(days=14))

            goods.append({
                "id": gid,
                "seller_id": seller_id,
                "name": name,
                "category": category,
                "price": price,
                "created_at": created_at,
            })
        return goods

    def generate_orders_and_items(user_ids, goods, n_orders=2000, target_items=5000):
        orders = []
        all_items = []
        item_id = 1
        avg_items_per_order = target_items / n_orders  # ~2.5

        for oid in range(1, n_orders + 1):
            # Seasonal bump: ~15% more in Nov-Dec
            # Pick a random date, then accept/reject based on month for weighting
            while True:
                created_at = random_datetime_between(START, NOW)
                month = created_at.month
                if month in (11, 12):
                    break  # always accept Nov-Dec
                elif random.random() < 0.87:  # reject ~13% of non-Nov-Dec to create the bump
                    break

            user_id = random.choice(user_ids)
            status = random.choice(ORDER_STATUSES)

            # Generate order items
            n_items = max(1, int(random.expovariate(1.0 / avg_items_per_order)))
            n_items = min(n_items, 8)  # cap at 8 items per order

            order_items = []
            for _ in range(n_items):
                good = random.choice(goods)
                quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]
                unit_price = good["price"]
                order_items.append({
                    "id": item_id,
                    "order_id": oid,
                    "good_id": good["id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                })
                item_id += 1

            total_amount = round(
                sum(i["quantity"] * float(i["unit_price"]) for i in order_items), 2,
            )

            orders.append({
                "id": oid,
                "user_id": user_id,
                "created_at": created_at,
                "status": status,
                "total_amount": total_amount,
            })
            all_items.extend(order_items)

        return orders, all_items

    def generate_ratings(user_ids, good_ids, n=3000):
        ratings = []
        seen = set()
        attempts = 0
        while len(ratings) < n and attempts < n * 3:
            attempts += 1
            user_id = random.choice(user_ids)
            good_id = random.choice(good_ids)
            key = (user_id, good_id)
            if key in seen:
                continue
            seen.add(key)
            ratings.append({
                "good_id": good_id,
                "user_id": user_id,
                "rating": random.choice(RATING_DISTRIBUTION),
                "created_at": random_datetime_between(START + timedelta(days=30), NOW),
            })
        return ratings

    def generate_reviews(ratings, n=1000):
        # Reviews are a subset of ratings
        selected = random.sample(ratings, min(n, len(ratings)))
        reviews = []
        for r in selected:
            if r["rating"] >= 4:
                title = random.choice(REVIEW_TITLES_GOOD)
                body = random.choice(REVIEW_BODIES_GOOD)
            elif r["rating"] <= 2:
                title = random.choice(REVIEW_TITLES_BAD)
                body = random.choice(REVIEW_BODIES_BAD)
            else:
                title = random.choice(REVIEW_TITLES_GOOD + REVIEW_TITLES_BAD)
                body = random.choice(REVIEW_BODIES_GOOD + REVIEW_BODIES_BAD)
            reviews.append({
                "good_id": r["good_id"],
                "user_id": r["user_id"],
                "title": title,
                "body": body,
                "created_at": random_datetime_between(r["created_at"], NOW),
            })
        return reviews


# ---------------------------------------------------------------------------
# Insert functions
# ---------------------------------------------------------------------------

def insert_mongo_users(db, users):
    db.users.drop()
    db.users.insert_many(users)
    log.info("MongoDB: inserted %d users", len(users))


def truncate_mysql(cursor):
    """Truncate MySQL tables so auto-increment resets for clean re-seeding."""
    cursor.execute("SET FOREIGN_KEY_CHECKS=0")
    for table in ["order_items", "orders", "goods", "sellers"]:
        cursor.execute(f"TRUNCATE TABLE {table}")
    cursor.execute("SET FOREIGN_KEY_CHECKS=1")
    log.info("MySQL: truncated existing data")


def insert_mysql_sellers(cursor, sellers):
    cursor.executemany(
        "INSERT INTO sellers (name, registered_at, country) VALUES (%s, %s, %s)",
        [(s["name"], s["registered_at"], s["country"]) for s in sellers],
    )
    log.info("MySQL: inserted %d sellers", len(sellers))


def insert_mysql_goods(cursor, goods):
    cursor.executemany(
        "INSERT INTO goods (seller_id, name, category, price, created_at) "
        "VALUES (%s, %s, %s, %s, %s)",
        [(g["seller_id"], g["name"], g["category"], g["price"], g["created_at"]) for g in goods],
    )
    log.info("MySQL: inserted %d goods", len(goods))


def insert_mysql_orders(cursor, orders):
    cursor.executemany(
        "INSERT INTO orders (user_id, created_at, status, total_amount) "
        "VALUES (%s, %s, %s, %s)",
        [(o["user_id"], o["created_at"], o["status"], o["total_amount"]) for o in orders],
    )
    log.info("MySQL: inserted %d orders", len(orders))


def insert_mysql_order_items(cursor, items):
    BATCH = 1000
    for i in range(0, len(items), BATCH):
        batch = items[i:i + BATCH]
        cursor.executemany(
            "INSERT INTO order_items (order_id, good_id, quantity, unit_price) "
            "VALUES (%s, %s, %s, %s)",
            [(it["order_id"], it["good_id"], it["quantity"], it["unit_price"]) for it in batch],
        )
    log.info("MySQL: inserted %d order items", len(items))


def truncate_mssql(cursor):
    """Truncate MSSQL tables for clean re-seeding."""
    for table in ["reviews", "goods_ratings"]:
        cursor.execute(f"TRUNCATE TABLE {table}")
    log.info("MSSQL: truncated existing data")


def insert_mssql_ratings(conn, ratings):
    conn.bulk_copy(
        "goods_ratings",
        [(r["good_id"], r["user_id"], r["rating"], r["created_at"]) for r in ratings],
        column_ids=[2, 3, 4, 5],
        batch_size=1000,
        tablock=True,
    )
    log.info("MSSQL: inserted %d ratings (bulk_copy)", len(ratings))


def insert_mssql_reviews(conn, reviews):
    conn.bulk_copy(
        "reviews",
        [(r["good_id"], r["user_id"], r["title"], r["body"], r["created_at"]) for r in reviews],
        column_ids=[2, 3, 4, 5, 6],
        batch_size=1000,
        tablock=True,
    )
    log.info("MSSQL: inserted %d reviews (bulk_copy)", len(reviews))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    random.seed(args.seed)

    mssql_schema = os.path.join(_SCRIPT_DIR, "init", "mssql", "01_schema.sql")

    log.info("Connecting to databases...")
    mongo_db = connect_mongo(args.mongo_host, args.mongo_port, args.mongo_db)
    mysql_conn = connect_mysql(
        args.mysql_host, args.mysql_port, args.mysql_user, args.mysql_password, args.mysql_db,
    )
    mssql_conn = connect_mssql(
        args.mssql_host, args.mssql_port, args.mssql_user, args.mssql_password,
    )

    log.info("Setting up MSSQL schema...")
    setup_mssql_schema(mssql_conn, args.mssql_db, mssql_schema)

    log.info("Generating data...")
    users = generate_users(args.users)
    user_ids = [u["user_id"] for u in users]
    sellers = generate_sellers(args.sellers)
    goods = generate_goods(sellers, args.goods)
    good_ids = [g["id"] for g in goods]
    orders, order_items = generate_orders_and_items(user_ids, goods, args.orders, args.order_items)
    ratings = generate_ratings(user_ids, good_ids, args.ratings)
    reviews = generate_reviews(ratings, args.reviews)

    log.info("Inserting data...")

    # MongoDB
    insert_mongo_users(mongo_db, users)

    # MySQL — truncate first so auto-increment resets to 1
    mysql_cur = mysql_conn.cursor()
    truncate_mysql(mysql_cur)
    insert_mysql_sellers(mysql_cur, sellers)
    insert_mysql_goods(mysql_cur, goods)
    insert_mysql_orders(mysql_cur, orders)
    insert_mysql_order_items(mysql_cur, order_items)
    mysql_cur.close()
    mysql_conn.close()

    # MSSQL
    mssql_conn.autocommit(True)
    mssql_cur = mssql_conn.cursor()
    mssql_cur.execute(f"USE [{args.mssql_db}]")
    truncate_mssql(mssql_cur)
    mssql_cur.close()
    insert_mssql_ratings(mssql_conn, ratings)
    insert_mssql_reviews(mssql_conn, reviews)
    mssql_conn.close()

    log.info(
        "Done! Users=%d  Sellers=%d  Goods=%d  Orders=%d  Items=%d  Ratings=%d  Reviews=%d",
        len(users), len(sellers), len(goods), len(orders),
        len(order_items), len(ratings), len(reviews),
    )


if __name__ == "__main__":
    main()
