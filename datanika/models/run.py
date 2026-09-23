import enum
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Enum, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin
from datanika.models.dependency import NodeType


class RunStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    #: A stop was requested and the worker has not confirmed it yet (`SPEC_RUN_CANCELLATION` §3).
    #: Non-terminal on purpose: a status reading `cancelled` while the warehouse is still being
    #: written is the same lie core#657 is about, moved one layer up.
    CANCELLING = "cancelling"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------------------------
# The status sets. Define once; every consumer derives (`SPEC_RUN_CANCELLATION` §4).
# ---------------------------------------------------------------------------------------------
#
# 🚨 **Adding a value to `RunStatus` is not a one-line change**, which is why these exist. Seven
# places used to enumerate these statuses by hand with nothing linking them, and two of those
# produced a confident wrong answer rather than an error:
#
#   * the `?wait=true` timeout branch tested `("pending", "running")` to mean *still going*, so a
#     non-terminal status added later fell through and returned `422 terminal-not-success` for a
#     run that was still working;
#   * `cleanup_orphaned_dlt_dirs` protected only `RUNNING`/`PENDING` directories, so any other
#     non-terminal status had its working directory deleted **while the worker was writing to
#     it** — live since `datanika-beat` began running that sweep hourly.
#
# Both follow from the same thing: a hand-maintained list cannot be wrong loudly.
#
# ⚠️ `TERMINAL` and `NON_TERMINAL` are written out rather than derived from each other. A
# complement (`set(RunStatus) - TERMINAL`) would silently make every future status non-terminal,
# which is the failure mode this replaces, one level up. The guard in
# `tests/test_models/test_run_status_sets_are_total.py` is what forces a deliberate choice: it
# fails when a status is in the enum and in neither set.

#: No further transition happens from here. A run in one of these is finished.
TERMINAL_RUN_STATUSES: frozenset[RunStatus] = frozenset(
    {RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED}
)

#: The run may still change on its own — something is, or should be, working on it.
NON_TERMINAL_RUN_STATUSES: frozenset[RunStatus] = frozenset(
    {RunStatus.PENDING, RunStatus.RUNNING, RunStatus.CANCELLING}
)

#: A cancel request is accepted in these. ⚠️ Equal to :data:`NON_TERMINAL_RUN_STATUSES` today and
#: kept separate anyway: they answer different questions, and a future status could be
#: non-terminal without being cancellable (a run mid-rollback, say). Deriving one from the other
#: would make that divergence impossible to express without first untangling them.
CANCELLABLE_RUN_STATUSES: frozenset[RunStatus] = frozenset(
    {RunStatus.PENDING, RunStatus.RUNNING, RunStatus.CANCELLING}
)

#: A stop has been asked for, whether or not the worker has acknowledged it. Cross-cutting rather
#: than a partition: `CANCELLING` is non-terminal and `CANCELLED` is terminal, and what these two
#: share is that a later report must not overwrite them with an ordinary outcome.
CANCEL_REQUESTED_RUN_STATUSES: frozenset[RunStatus] = frozenset(
    {RunStatus.CANCELLING, RunStatus.CANCELLED}
)


class CatalogSyncVerdict(enum.StrEnum):
    """What an upload run's catalogue sync found (core#1398, ``SPEC_EARNED_VERDICTS`` §4.6).

    Stored as its value in a plain string column, never through ``Enum``: a non-native ``Enum``
    without ``values_callable`` stores member NAMES (core#1391's class).
    """

    #: The sync found tables, and they are in the catalogue.
    CATALOGUED = "catalogued"
    #: No rows and no tables: a legitimately empty load (core#883's control).
    EMPTY = "empty"
    #: Rows loaded and the sync found no tables in the schema it asked.
    NO_TABLES = "no_tables"
    #: The sync raised: the catalogue could not read the destination.
    UNREADABLE = "unreadable"


class Run(Base, TenantMixin, TimestampMixin):
    __tablename__ = "runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    target_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    target_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[RunStatus] = mapped_column(
        Enum(RunStatus, native_enum=False, length=20),
        nullable=False,
        default=RunStatus.PENDING,
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    logs: Mapped[str | None] = mapped_column(Text, nullable=True)
    # BigInteger: Enterprise clickstream/event/log backfills routinely
    # exceed 2^31 rows in a single pipeline run. int32 would overflow on
    # insert with ``NumericValueOutOfRange``. See core#283 — same class
    # as the usage_ledger.quantity widening in core#272.
    rows_loaded: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # core#912. Core computes this on every upload — from dlt's `LoadInfo` on the
    # ETL path, from `StreamStats.bytes_out` on the ELT one — hands it to a hook
    # and keeps no record, so `Run` could answer "how many rows" and never "how
    # large". That made `datanika_bytes_processed_by_run` underivable: the value
    # is known in the Celery worker, `/metrics` is served by the app, and core
    # must never import cloud to read `usage_ledger`.
    #
    # `BigInteger` for core#283's reason one line up, only sooner: 2 GiB in a
    # single load is unremarkable, and int32 would raise
    # `NumericValueOutOfRange` on insert *after* the data had already moved.
    #
    # NULL means "not measured" — a run predating this column, or one whose
    # LoadInfo carried no file sizes. Writing 0 there would erase that
    # distinction and put a fake floor in every distribution built on it.
    bytes_processed: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    # core#1398. What this run's catalogue sync found, as a `CatalogSyncVerdict` value, and for
    # `no_tables` the schema it asked. `/models` reads the most recent successful run of each
    # upload. NULL means not recorded: a run from before migration `j0k1l2m3n4o5`, or not an upload.
    catalog_sync_verdict: Mapped[str | None] = mapped_column(String(32), nullable=True)
    catalog_sync_schema: Mapped[str | None] = mapped_column(String(255), nullable=True)
