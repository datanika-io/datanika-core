#!/usr/bin/env python3
"""Cross-org FK audit: find rows whose FK points at a row owned by a DIFFERENT org.

Run inside the app container:

    docker exec -i datanika-app /app/.venv/bin/python /dev/stdin < cross_org_fk_audit.py

WHY THE COVERAGE SELF-CHECK IS THE POINT
----------------------------------------
A cross-org audit that examines zero FK pairs prints "0 violations" and reads exactly like a
clean bill of health. So does one that examines ten pairs whose child tables are all empty --
which is the real situation here: `pipelines` and `transformations` have never held a row, so a
cross-org reference between them is *arithmetically impossible* and a clean result about them
says nothing at all.

This script therefore reports, and makes you look at:

  * how many `org_id`-bearing tables exist,
  * how many FK pairs were actually examined (exit 2 if that is zero),
  * the row count of every child table, with pairs over EMPTY children reported separately as
    VACUOUS rather than folded into the clean total.

The headline number is therefore "N violations across P pairs, of which V were vacuous", never a
bare zero.
"""

import sys

from sqlalchemy import text

from datanika.db import sync_engine

FK_SQL = text("""
SELECT con.conname,
       child.relname  AS child_table,
       parent.relname AS parent_table,
       (SELECT array_agg(a.attname ORDER BY u.ord)
          FROM unnest(con.conkey) WITH ORDINALITY AS u(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = u.attnum) AS child_cols,
       (SELECT array_agg(a.attname ORDER BY u.ord)
          FROM unnest(con.confkey) WITH ORDINALITY AS u(attnum, ord)
          JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = u.attnum) AS parent_cols
  FROM pg_constraint con
  JOIN pg_class child   ON child.oid  = con.conrelid
  JOIN pg_class parent  ON parent.oid = con.confrelid
  JOIN pg_namespace n   ON n.oid      = child.relnamespace
 WHERE con.contype = 'f' AND n.nspname = 'public'
 ORDER BY child.relname, con.conname
""")

ORG_TABLES_SQL = text("""
SELECT table_name FROM information_schema.columns
 WHERE table_schema = 'public' AND column_name = 'org_id'
 ORDER BY table_name
""")


def main() -> int:
    with sync_engine.connect() as conn:
        org_tables = {r[0] for r in conn.execute(ORG_TABLES_SQL)}
        print(f"org_id-bearing tables: {len(org_tables)}")
        print("  " + ", ".join(sorted(org_tables)))

        counts = {}
        for t in sorted(org_tables):
            counts[t] = conn.execute(text(f'SELECT count(*) FROM "{t}"')).scalar_one()
        print("\nrow counts:")
        for t, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"  {t:<28} {c}")

        fks = list(conn.execute(FK_SQL))
        pairs, vacuous, violations, total_checked_rows = [], [], [], 0

        for conname, child, parent, ccols, pcols in fks:
            if child not in org_tables or parent not in org_tables:
                continue
            if len(ccols) != 1 or len(pcols) != 1:
                print(f"  SKIP composite FK {conname} ({child}->{parent})")
                continue
            ccol, pcol = ccols[0], pcols[0]
            if ccol == "org_id":  # the tenancy column itself, not a tenant-owned ref
                continue
            pairs.append((conname, child, parent, ccol, pcol))

        if not pairs:
            print("\n[COVERAGE FAILURE] zero FK pairs examined — this result is not evidence.")
            return 2

        print(f"\nFK pairs examined: {len(pairs)}")
        for conname, child, parent, ccol, pcol in pairs:
            n_child = counts.get(child, 0)
            q = text(
                f'SELECT count(*) FROM "{child}" c '
                f'JOIN "{parent}" p ON p."{pcol}" = c."{ccol}" '
                f'WHERE c."{ccol}" IS NOT NULL AND c.org_id IS DISTINCT FROM p.org_id'
            )
            bad = conn.execute(q).scalar_one()
            tag = "VACUOUS (child empty)" if n_child == 0 else f"checked {n_child} rows"
            print(f"  {child}.{ccol} -> {parent}.{pcol:<4} bad={bad:<4} [{tag}]  ({conname})")
            if n_child == 0:
                vacuous.append((child, parent))
            else:
                total_checked_rows += n_child
            if bad:
                violations.append((child, ccol, parent, pcol, bad))

    print("\n" + "=" * 72)
    print(
        f"RESULT: {len(violations)} violation(s) across {len(pairs)} FK pair(s); "
        f"{len(vacuous)} pair(s) VACUOUS (child table empty)"
    )
    print(f"        {total_checked_rows} child row(s) actually compared")
    if vacuous:
        print("\n  ⚠️  Vacuous pairs prove nothing. They are listed so a clean total is not")
        print("      mistaken for coverage:")
        for child, parent in vacuous:
            print(f"        {child} -> {parent}")
    if violations:
        print("\n  🚨 CROSS-ORG REFERENCES FOUND:")
        for child, ccol, parent, pcol, bad in violations:
            print(f"        {bad} row(s): {child}.{ccol} -> {parent}.{pcol}")
        return 1
    if total_checked_rows == 0:
        print("\n  [COVERAGE FAILURE] every pair was vacuous — 0 rows were compared.")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
