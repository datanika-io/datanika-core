# UX Spec — Contextual Tooltips (Onboarding Slice 3)

> **Status**: spec draft — not yet coded
> **Tracks**: `PLAN_PRODUCT.md` → P0 Onboarding Experience → bullet 3
> **Author**: Product agent, 2026-04-12

---

## Problem

New users encounter domain-specific jargon on the Uploads and Transformations pages without explanation. Terms like "write disposition", "materialization", "incremental cursor", and "schema contract" are standard in data engineering but opaque to first-time users, product analysts, or junior engineers. The Getting Started checklist (PR #51) and empty states (PR #59) guide the *where*; tooltips guide the *what*.

## Scope

**In scope**: a small, reusable tooltip component and 8 concrete tooltip placements.
**Out of scope**: in-depth docs pages for each concept (those belong in `/docs/`), inline walkthroughs/tours, marketing-site tooltips.

---

## Concepts to explain (8 tooltips)

Identified from a codebase scan of `ui/pages/uploads.py` and `ui/pages/transformations.py`:

| # | Concept | Where it appears | i18n key |
|---|---------|-----------------|----------|
| 1 | **Write disposition** | Uploads page — select (`append`, `replace`, `merge`) | `tooltip.write_disposition` |
| 2 | **Append** | Uploads page — option in write disposition select | `tooltip.write_disposition_append` |
| 3 | **Replace** | Uploads page — option in write disposition select | `tooltip.write_disposition_replace` |
| 4 | **Merge** | Uploads page — option in write disposition select | `tooltip.write_disposition_merge` |
| 5 | **Incremental cursor** | Uploads page — cursor_path input (visible when incremental enabled) | `tooltip.incremental_cursor` |
| 6 | **Schema contract** | Uploads page — schema_contract select | `tooltip.schema_contract` |
| 7 | **Load mode** | Uploads page — select (`full_database`, `single_table`) | `tooltip.load_mode` |
| 8 | **Materialization** | Transformations page — select (`view`, `table`, `incremental`, `ephemeral`) | `tooltip.materialization` |

### Copy (English — other locales in i18n pass)

1. **Write disposition** — "Controls how Datanika writes data to the destination table on each run. Choose append (add rows), replace (drop and reload), or merge (upsert changed rows by primary key)."
2. **Append** — "Adds new rows to the destination table without touching existing data. Best for event logs and append-only tables."
3. **Replace** — "Drops and recreates the destination table on every run. Simple and safe for small lookup tables, but re-processes everything."
4. **Merge** — "Upserts rows by primary key — inserts new rows and updates existing ones. Requires a primary key. Best for large tables where only some rows change between runs."
5. **Incremental cursor** — "A column that only ever increases (e.g., updated_at, id). Datanika uses it to fetch only new or changed rows since the last run, instead of scanning the full source table."
6. **Schema contract** — "Controls what happens when the source schema changes (new columns, type changes). Options: evolve (auto-adapt), freeze (reject changes), discard_value (keep schema, null new columns)."
7. **Load mode** — "full_database syncs all tables from the source in one pipeline. single_table syncs one table at a time with fine-grained control over write disposition and primary key."
8. **Materialization** — "Controls how dbt builds this model in the warehouse. view = SQL view (fast, no storage). table = physical table (slower build, fast query). incremental = append/merge new rows only. ephemeral = inline CTE (no object created)."

---

## Component standard

### Recommendation: Radix Tooltip via `rx.tooltip`

Reflex wraps Radix UI primitives. `rx.tooltip` is already available — no new dependency. Pattern:

```python
rx.tooltip(
    rx.icon("help-circle", size=14, color="var(--slate-8)"),
    content=_t["tooltip.write_disposition"],
    side="right",
)
```

### Rendering pattern

Place the tooltip icon **inline next to the label**, not on the input itself:

```
Write Disposition ⓘ          ← label + tooltip trigger
[  append  ▾ ]                ← select unchanged
```

This keeps the form layout clean and works across input types (select, text input, checkbox).

### Reusable helper

```python
# datanika/ui/components/info_tooltip.py

def info_tooltip(i18n_key: str) -> rx.Component:
    return rx.tooltip(
        rx.icon("help-circle", size=14, color="var(--slate-8)", cursor="help"),
        content=_t[i18n_key],
        side="right",
        max_width="320px",
    )
```

Pages then import and use:

```python
rx.hstack(
    rx.text(_t["uploads.write_disposition"], size="2", weight="bold"),
    info_tooltip("tooltip.write_disposition"),
    align="center",
    spacing="1",
)
```

---

## Implementation plan

1. **New component**: `datanika/ui/components/info_tooltip.py` — `info_tooltip(i18n_key)` helper
2. **i18n keys**: 8 keys in all 9 locales (`tooltip.*`)
3. **Wire into uploads.py**: 7 placements (write_disposition, append, replace, merge, incremental_cursor, schema_contract, load_mode)
4. **Wire into transformations.py**: 1 placement (materialization)
5. **TDD**: test that `info_tooltip()` returns a component, and that all 8 i18n keys exist in the orphan-key scanner

### Estimated scope

- 1 new file (`info_tooltip.py`) — ~15 lines
- 2 modified pages (`uploads.py`, `transformations.py`) — ~8 insertions each
- 9 locale files × 8 new keys = 72 string insertions
- 1 new test file — ~10 tests (component smoke + key shape)
- No migrations, no state classes, no new deps

### What this does NOT include

- **Tooltip on every label** — only the 8 jargon-heavy concepts above. Adding more later is trivial (one `info_tooltip()` call per label).
- **Guided tour / walkthrough** — that's a bigger UX piece and would need a tour framework. Tooltips are a lighter, always-available affordance.
- **Tooltip analytics** — no tracking of which tooltips users hover. Can be added later if product analytics is wired.

---

## Decision needed before coding

1. **Side preference**: `right` (default) or `top`? Right avoids clipping in narrow sidebar layouts but may collide with input fields. Top is safer for forms but less conventional.
2. **Tooltip on option-level or select-level?** For write disposition, should each option (`append`, `replace`, `merge`) have its own tooltip inside the dropdown, or should one tooltip on the label explain all three? Current spec proposes label-level only (simpler, no custom dropdown rendering). Option-level requires a custom select component.

**Recommendation**: label-level tooltip explaining all three options in one block. Simpler, works with the existing `searchable_select` component, and one tooltip is less visually noisy than three.

---

## Acceptance criteria

- [ ] `info_tooltip(i18n_key)` renders an `rx.tooltip` with a help-circle icon
- [ ] 8 tooltip placements across uploads.py and transformations.py
- [ ] 8 i18n keys in all 9 locales
- [ ] `test_all_locales_have_same_keys` green
- [ ] `test_no_orphan_keys_in_json` green
- [ ] Full suite green, ruff clean
- [ ] No new dependencies
