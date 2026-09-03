import enum
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Enum, Text
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin
from datanika.models.dependency import NodeType


class RunStatus(enum.StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


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
