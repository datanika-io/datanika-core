import enum
import uuid
from datetime import datetime

from sqlalchemy import DateTime, Enum, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from etlfabric.models.base import Base, TenantMixin, TimestampMixin, UUIDType
from etlfabric.models.dependency import NodeType


class RunStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Run(Base, TenantMixin, TimestampMixin):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(), primary_key=True, default=uuid.uuid4
    )
    target_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    target_id: Mapped[uuid.UUID] = mapped_column(UUIDType(), nullable=False)
    status: Mapped[RunStatus] = mapped_column(
        Enum(RunStatus, native_enum=False, length=20),
        nullable=False,
        default=RunStatus.PENDING,
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    logs: Mapped[str | None] = mapped_column(Text, nullable=True)
    rows_loaded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
