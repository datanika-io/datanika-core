import enum
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Enum, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin
from datanika.models.dependency import NodeType


class RunStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


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
