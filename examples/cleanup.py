"""
Cleanup script for Datanika and the Online Store example databases.

Truncates all Datanika tables (PostgreSQL), example tables (MySQL, MSSQL),
and drops the MongoDB users collection.  Containers stay running — only data
is removed.
"""

import argparse
import configparser
import os
import time

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
    p = argparse.ArgumentParser(description="Wipe all data from Datanika and example databases")

    # PostgreSQL (Datanika)
    p.add_argument("--pg-host", default=os.getenv("PG_HOST", conf.get("postgresql", "host", fallback="localhost")))
    p.add_argument("--pg-port", type=int, default=int(os.getenv("PG_PORT", conf.get("postgresql", "port", fallback="5432"))))
    p.add_argument("--pg-user", default=os.getenv("PG_USER", conf.get("postgresql", "user", fallback="datanika")))
    p.add_argument("--pg-password", default=os.getenv("PG_PASSWORD", conf.get("postgresql", "password", fallback="datanika")))
    p.add_argument("--pg-db", default=os.getenv("PG_DB", conf.get("postgresql", "database", fallback="datanika")))

    # MySQL (example)
    p.add_argument("--mysql-host", default=os.getenv("MYSQL_HOST", conf.get("mysql", "host", fallback="localhost")))
    p.add_argument("--mysql-port", type=int, default=int(os.getenv("MYSQL_PORT", conf.get("mysql", "port", fallback="3306"))))
    p.add_argument("--mysql-user", default=os.getenv("MYSQL_USER", conf.get("mysql", "user", fallback="root")))
    p.add_argument("--mysql-password", default=os.getenv("MYSQL_PASSWORD", conf.get("mysql", "password", fallback="root")))
    p.add_argument("--mysql-db", default=os.getenv("MYSQL_DB", conf.get("mysql", "database", fallback="online_store")))

    # MSSQL (example)
    p.add_argument("--mssql-host", default=os.getenv("MSSQL_HOST", conf.get("mssql", "host", fallback="localhost")))
    p.add_argument("--mssql-port", type=int, default=int(os.getenv("MSSQL_PORT", conf.get("mssql", "port", fallback="1433"))))
    p.add_argument("--mssql-user", default=os.getenv("MSSQL_USER", conf.get("mssql", "user", fallback="sa")))
    p.add_argument("--mssql-password", default=os.getenv("MSSQL_PASSWORD", conf.get("mssql", "password", fallback="SA_Password1!")))
    p.add_argument("--mssql-db", default=os.getenv("MSSQL_DB", conf.get("mssql", "database", fallback="online_store")))

    # MongoDB (example)
    p.add_argument("--mongo-host", default=os.getenv("MONGO_HOST", conf.get("mongodb", "host", fallback="localhost")))
    p.add_argument("--mongo-port", type=int, default=int(os.getenv("MONGO_PORT", conf.get("mongodb", "port", fallback="27017"))))
    p.add_argument("--mongo-db", default=os.getenv("MONGO_DB", conf.get("mongodb", "database", fallback="online_store")))

    return p.parse_args()


# ---------------------------------------------------------------------------
# Connection helpers with retry
# ---------------------------------------------------------------------------

CONNECT_TIMEOUT = 30  # seconds


def connect_pg(host, port, user, password, db):
    import psycopg2

    deadline = time.time() + CONNECT_TIMEOUT
    while True:
        try:
            conn = psycopg2.connect(
                host=host, port=port, user=user, password=password, dbname=db,
            )
            conn.autocommit = True
            print(f"  PostgreSQL connected ({host}:{port})")
            return conn
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError(f"PostgreSQL connection timed out: {exc}") from exc
            time.sleep(1)


def connect_mysql(host, port, user, password, db):
    import pymysql

    deadline = time.time() + CONNECT_TIMEOUT
    while True:
        try:
            conn = pymysql.connect(
                host=host, port=port, user=user, password=password, database=db,
                charset="utf8mb4", autocommit=True,
            )
            print(f"  MySQL connected ({host}:{port})")
            return conn
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError(f"MySQL connection timed out: {exc}") from exc
            time.sleep(1)


def connect_mssql(host, port, user, password, db):
    import pymssql

    deadline = time.time() + CONNECT_TIMEOUT
    while True:
        try:
            conn = pymssql.connect(
                server=host, port=port, user=user, password=password, database=db,
                autocommit=True,
            )
            print(f"  MSSQL connected ({host}:{port})")
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
            print(f"  MongoDB connected ({host}:{port})")
            return client[db]
        except Exception as exc:
            if time.time() > deadline:
                raise RuntimeError(f"MongoDB connection timed out: {exc}") from exc
            time.sleep(1)


# ---------------------------------------------------------------------------
# Cleanup functions
# ---------------------------------------------------------------------------

# Datanika PostgreSQL — all PUBLIC_TABLES from migrations/helpers.py
PG_TABLES = [
    "audit_logs",
    "catalog_entries",
    "uploaded_files",
    "api_keys",
    "dependencies",
    "runs",
    "schedules",
    "transformations",
    "uploads",
    "pipelines",
    "connections",
    "memberships",
    "users",
    "organizations",
]


def cleanup_pg(conn):
    cur = conn.cursor()
    # Bypass FK checks for the session
    cur.execute("SET session_replication_role = 'replica'")
    truncated = 0
    for table in PG_TABLES:
        cur.execute(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = %s)",
            (table,),
        )
        if cur.fetchone()[0]:
            cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
            truncated += 1
    cur.execute("SET session_replication_role = 'origin'")
    cur.close()
    print(f"  PostgreSQL: truncated {truncated}/{len(PG_TABLES)} tables")


# MySQL (example) — reverse FK order, but FK checks disabled anyway
MYSQL_TABLES = ["order_items", "orders", "goods", "sellers"]


def cleanup_mysql(conn):
    cur = conn.cursor()
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    for table in MYSQL_TABLES:
        cur.execute(f"TRUNCATE TABLE {table}")
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    cur.close()
    print(f"  MySQL: truncated {', '.join(MYSQL_TABLES)}")


# MSSQL (example) — no FK constraints
MSSQL_TABLES = ["reviews", "goods_ratings"]


def cleanup_mssql(conn):
    cur = conn.cursor()
    for table in MSSQL_TABLES:
        cur.execute(f"TRUNCATE TABLE {table}")
    cur.close()
    print(f"  MSSQL: truncated {', '.join(MSSQL_TABLES)}")


def cleanup_mongo(db):
    db.users.drop()
    print("  MongoDB: dropped users collection")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    print("Connecting to databases...")
    pg_conn = connect_pg(
        args.pg_host, args.pg_port, args.pg_user, args.pg_password, args.pg_db,
    )
    mysql_conn = connect_mysql(
        args.mysql_host, args.mysql_port, args.mysql_user, args.mysql_password, args.mysql_db,
    )
    mssql_conn = connect_mssql(
        args.mssql_host, args.mssql_port, args.mssql_user, args.mssql_password, args.mssql_db,
    )
    mongo_db = connect_mongo(args.mongo_host, args.mongo_port, args.mongo_db)

    print("\nCleaning up Datanika (PostgreSQL)...")
    cleanup_pg(pg_conn)

    print("\nCleaning up example databases...")
    cleanup_mysql(mysql_conn)
    cleanup_mssql(mssql_conn)
    cleanup_mongo(mongo_db)

    pg_conn.close()
    mysql_conn.close()
    mssql_conn.close()

    print("\nAll databases cleaned.")


if __name__ == "__main__":
    main()
