# Online Store Example Dataset

A multi-database example dataset demonstrating Datanika's cross-database ELT capabilities. Data is spread across three databases:

| Database | Contents |
|----------|----------|
| **MongoDB** | User profiles (500 users) |
| **MySQL** | Sellers, goods, orders, order items (50 sellers, 500 goods, 2000 orders, ~5000 items) |
| **MSSQL** | Ratings and reviews (3000 ratings, 1000 reviews) |

## Prerequisites

- Docker and Docker Compose
- The main Datanika stack running (`docker compose up -d` from `datanika/`)
- Python 3.12+ with the Datanika virtualenv activated (includes `pymysql`, `pymssql`, `pymongo`)

## Quick Start

```bash
# 1. Make sure the main Datanika stack is running (creates the shared network)
cd datanika
docker compose up -d

# 2. Start the example databases (joins the main network)
cd examples
docker compose up -d

# 3. Wait for all containers to be healthy
docker compose ps

# 4. Seed the data (from the datanika project root)
cd ..
.venv/Scripts/python examples/seed_data.py
```

## Verification

```bash
# MySQL — check order count
docker exec examples-mysql-1 mysql -uroot -proot online_store -e "SELECT COUNT(*) FROM orders;"

# MSSQL — check review count (via Python, since sqlcmd auth can be flaky in Docker)
python -c "import pymssql; c=pymssql.connect('localhost',1433,'sa','SA_Password1!','online_store'); print(c.cursor().execute('SELECT COUNT(*) FROM reviews').fetchone())"

# MongoDB — check user count
docker exec examples-mongodb-1 mongosh online_store --eval "db.users.countDocuments()"
```

## Configuration

Connection defaults for all databases live in `databases.conf` (INI format). Both `seed_data.py` and `cleanup.py` read from this file automatically.

```ini
[mysql]
host = localhost
port = 3306
user = root
password = root
database = online_store
```

Edit the file to match your environment — no need to pass `--mysql-host`, `--pg-port`, etc. every time.

**Override priority** (highest wins): CLI args > environment variables > `databases.conf` > hardcoded fallbacks.

## Connection Details

The example containers join the `datanika_default` network, so the main app and Celery containers can reach them by service name.

**From Datanika containers** (app, celery):

| Database | Host | Port | User | Password | Database |
|----------|------|------|------|----------|----------|
| MySQL | mysql | 3306 | root | root | online_store |
| MSSQL | mssql | 1433 | sa | SA_Password1! | online_store |
| MongoDB | mongodb | 27017 | — | — | online_store |

**From host machine** (seed script, debugging):

| Database | Host | Port | User | Password | Database |
|----------|------|------|------|----------|----------|
| MySQL | localhost | 3306 | root | root | online_store |
| MSSQL | localhost | 1433 | sa | SA_Password1! | online_store |
| MongoDB | localhost | 27017 | — | — | online_store |

## Custom Data Volumes

The seed script accepts CLI args to control how many records are generated (defaults match the table above):

```bash
python examples/seed_data.py --users 100 --sellers 10 --goods 50 --orders 200 --order-items 500 --ratings 300 --reviews 100
```

| Flag | Default | Notes |
|------|---------|-------|
| `--users` | 500 | MongoDB users |
| `--sellers` | 50 | Capped at 50 (fixed name list) |
| `--goods` | 500 | |
| `--orders` | 2000 | |
| `--order-items` | 5000 | Target, not exact |
| `--ratings` | 3000 | |
| `--reviews` | 1000 | Subset of ratings |

## Cleanup (reset data)

Wipe all data from **every** database — Datanika (PostgreSQL) and the three example databases — without recreating containers:

```bash
python examples/cleanup.py
```

This:
- **PostgreSQL**: truncates all 14 Datanika tables (organizations, users, connections, pipelines, etc.) with `RESTART IDENTITY`
- **MySQL**: truncates sellers, goods, orders, order_items
- **MSSQL**: truncates goods_ratings, reviews
- **MongoDB**: drops the users collection

Accepts `--pg-*`, `--mysql-*`, `--mssql-*`, and `--mongo-*` connection args (defaults match `docker-compose.yml`).

## Cleanup (destroy containers)

```bash
cd datanika/examples
docker compose down -v
```
