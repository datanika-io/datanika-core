# cython: language_level=3, boundscheck=False, wraparound=False
"""
Cython-optimized data generators for the seed script.

Compile with:  python build_cython.py build_ext --inplace
"""

import json
import os
import random
import string
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Load constants (same logic as seed_data.py)
# ---------------------------------------------------------------------------

cdef str _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(_SCRIPT_DIR, "seed_constants.json")) as _f:
    _C = json.load(_f)

cdef list FIRST_NAMES = _C["first_names"]
cdef list LAST_NAMES = _C["last_names"]
cdef list CITIES = _C["cities"]
cdef list STREETS = _C["streets"]
cdef list SELLER_NAMES = _C["seller_names"]
cdef list COUNTRIES = list(set(_C["countries"]))
cdef list CATEGORIES = _C["categories"]
cdef dict PRODUCT_NOUNS = _C["product_nouns"]
cdef list PRODUCT_ADJECTIVES = _C["product_adjectives"]
cdef list EMAIL_DOMAINS = _C["email_domains"]
cdef list REVIEW_TITLES_GOOD = _C["review_titles_good"]
cdef list REVIEW_TITLES_BAD = _C["review_titles_bad"]
cdef list REVIEW_BODIES_GOOD = _C["review_bodies_good"]
cdef list REVIEW_BODIES_BAD = _C["review_bodies_bad"]

cdef list ORDER_STATUSES = [s for s, w in _C["order_statuses"].items() for _ in range(w)]
cdef list RATING_DISTRIBUTION = [int(r) for r, w in _C["rating_distribution"].items() for _ in range(w)]

# Pre-compute combined review lists for rating == 3
cdef list _REVIEW_TITLES_ALL = REVIEW_TITLES_GOOD + REVIEW_TITLES_BAD
cdef list _REVIEW_BODIES_ALL = REVIEW_BODIES_GOOD + REVIEW_BODIES_BAD

# Time range: 18 months ending "now" (fixed for reproducibility)
NOW = datetime(2025, 6, 15, 12, 0, 0)
START = NOW - timedelta(days=18 * 30)

# Pre-bind frequently used functions
cdef object _random = random.random
cdef object _randint = random.randint
cdef object _choice = random.choice
cdef object _choices = random.choices
cdef object _sample = random.sample

# Pre-compute some timedeltas
cdef object _DELTA_30 = timedelta(days=30)
cdef object _DELTA_7 = timedelta(days=7)
cdef object _DELTA_60 = timedelta(days=60)
cdef object _DELTA_14 = timedelta(days=14)

# Quantity weights for order items
cdef list _QTY_POPULATION = [1, 2, 3, 4, 5]
cdef list _QTY_WEIGHTS = [50, 25, 15, 7, 3]

# Email separators
cdef list _EMAIL_SEPS = [".", "_", ""]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

cpdef str random_phone():
    return f"+1-{_randint(200, 999)}-{_randint(100, 999)}-{_randint(1000, 9999)}"


cpdef str random_email(str first, str last):
    cdef str sep = _choice(_EMAIL_SEPS)
    cdef str suffix = str(_randint(1, 999)) if _random() < 0.4 else ""
    return f"{first.lower()}{sep}{last.lower()}{suffix}@{_choice(EMAIL_DOMAINS)}"


cpdef object random_datetime_between(object start, object end):
    cdef double delta = (end - start).total_seconds()
    return start + timedelta(seconds=_random() * delta)


cdef str _random_suffix():
    """Generate 1-3 random uppercase letters, e.g. 'GRD', 'DF', 'A'."""
    return "".join(random.choices(string.ascii_uppercase, k=_randint(1, 3)))


# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

cpdef list generate_users(int n):
    cdef list users = []
    cdef int uid
    cdef str first, last
    cdef object created, last_login
    cdef object active_start = NOW - _DELTA_7
    cdef object reg_end = NOW - _DELTA_30
    cdef object users_append = users.append

    for uid in range(1, n + 1):
        first = _choice(FIRST_NAMES)
        last = _choice(LAST_NAMES)
        created = random_datetime_between(START, reg_end)
        if _random() < 0.6:
            last_login = random_datetime_between(active_start, NOW)
        else:
            last_login = random_datetime_between(created, NOW)
        users_append({
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
                "city": _choice(CITIES),
                "street": _choice(STREETS),
                "building": str(_randint(1, 200)),
            },
        })
    return users


cpdef list generate_sellers(int n):
    cdef list sellers = []
    cdef set used_names = set()
    cdef int sid, j
    cdef str base, country, name
    cdef object reg_end = NOW - _DELTA_60
    cdef object sellers_append = sellers.append

    for sid in range(1, n + 1):
        base = _choice(SELLER_NAMES)
        country = _choice(COUNTRIES)
        if base not in used_names:
            name = base
        else:
            for j in range(1000):
                name = f"{base} {_random_suffix()} {country}"
                if name not in used_names:
                    break
        used_names.add(name)
        sellers_append({
            "id": sid,
            "name": name,
            "registered_at": random_datetime_between(START, reg_end),
            "country": country,
        })
    return sellers


cpdef list generate_goods(list sellers, int n):
    cdef Py_ssize_t top_count = max(1, len(sellers) // 5)
    cdef list top_seller_ids = [s["id"] for s in sellers[:top_count]]
    cdef list other_seller_ids = [s["id"] for s in sellers[top_count:]]

    cdef list goods = []
    cdef int gid, seller_id
    cdef str category, noun, adj, name
    cdef double price
    cdef object created_at
    cdef object goods_end = NOW - _DELTA_14
    cdef object goods_append = goods.append

    for gid in range(1, n + 1):
        if _random() < 0.6:
            seller_id = _choice(top_seller_ids)
        else:
            seller_id = _choice(other_seller_ids)

        category = _choice(CATEGORIES)
        noun = _choice(PRODUCT_NOUNS[category])
        adj = _choice(PRODUCT_ADJECTIVES)
        name = f"{adj} {noun}"
        price = round(_random() * 495.0 + 5.0, 2)
        created_at = random_datetime_between(START, goods_end)

        goods_append({
            "id": gid,
            "seller_id": seller_id,
            "name": name,
            "category": category,
            "price": price,
            "created_at": created_at,
        })
    return goods


cpdef tuple generate_orders_and_items(list user_ids, list goods, int n_orders, int target_items):
    cdef list orders = []
    cdef list all_items = []
    cdef int item_id = 1
    cdef double avg_items_per_order = <double>target_items / <double>n_orders

    cdef int oid, n_items, user_id, month, quantity
    cdef double unit_price, total_amount
    cdef str status
    cdef object created_at, good
    cdef list order_items
    cdef object orders_append = orders.append
    cdef object items_extend = all_items.extend

    for oid in range(1, n_orders + 1):
        while True:
            created_at = random_datetime_between(START, NOW)
            month = created_at.month
            if month == 11 or month == 12:
                break
            elif _random() < 0.87:
                break

        user_id = _choice(user_ids)
        status = _choice(ORDER_STATUSES)

        n_items = max(1, int(random.expovariate(1.0 / avg_items_per_order)))
        if n_items > 8:
            n_items = 8

        order_items = []
        total_amount = 0.0
        for _ in range(n_items):
            good = _choice(goods)
            quantity = _choices(_QTY_POPULATION, weights=_QTY_WEIGHTS)[0]
            unit_price = good["price"]
            order_items.append({
                "id": item_id,
                "order_id": oid,
                "good_id": good["id"],
                "quantity": quantity,
                "unit_price": unit_price,
            })
            total_amount += quantity * <double>unit_price
            item_id += 1

        total_amount = round(total_amount, 2)

        orders_append({
            "id": oid,
            "user_id": user_id,
            "created_at": created_at,
            "status": status,
            "total_amount": total_amount,
        })
        items_extend(order_items)

    return orders, all_items


cpdef list generate_ratings(list user_ids, list good_ids, int n):
    cdef list ratings = []
    cdef set seen = set()
    cdef int attempts = 0
    cdef int max_attempts = n * 3
    cdef int user_id, good_id, rating
    cdef tuple key
    cdef object ratings_start = START + _DELTA_30
    cdef object ratings_append = ratings.append

    while len(ratings) < n and attempts < max_attempts:
        attempts += 1
        user_id = _choice(user_ids)
        good_id = _choice(good_ids)
        key = (user_id, good_id)
        if key in seen:
            continue
        seen.add(key)
        ratings_append({
            "good_id": good_id,
            "user_id": user_id,
            "rating": _choice(RATING_DISTRIBUTION),
            "created_at": random_datetime_between(ratings_start, NOW),
        })
    return ratings


cpdef list generate_reviews(list ratings, int n):
    cdef list selected = _sample(ratings, min(n, len(ratings)))
    cdef list reviews = []
    cdef int rating_val
    cdef str title, body
    cdef dict r
    cdef object reviews_append = reviews.append

    for r in selected:
        rating_val = r["rating"]
        if rating_val >= 4:
            title = _choice(REVIEW_TITLES_GOOD)
            body = _choice(REVIEW_BODIES_GOOD)
        elif rating_val <= 2:
            title = _choice(REVIEW_TITLES_BAD)
            body = _choice(REVIEW_BODIES_BAD)
        else:
            title = _choice(_REVIEW_TITLES_ALL)
            body = _choice(_REVIEW_BODIES_ALL)
        reviews_append({
            "good_id": r["good_id"],
            "user_id": r["user_id"],
            "title": title,
            "body": body,
            "created_at": random_datetime_between(r["created_at"], NOW),
        })
    return reviews
