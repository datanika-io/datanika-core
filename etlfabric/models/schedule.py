import uuid

from sqlalchemy import Boolean, Enum, String
from sqlalchemy.orm import Mapped, mapped_column

from etlfabric.models.base import Base, TenantMixin, TimestampMixin, UUIDType
from etlfabric.models.dependency import NodeType


class Schedule(Base, TenantMixin, TimestampMixin):
    __tablename__ = "schedules"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(), primary_key=True, default=uuid.uuid4
    )
    target_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    target_id: Mapped[uuid.UUID] = mapped_column(UUIDType(), nullable=False)
    cron_expression: Mapped[str] = mapped_column(String(100), nullable=False)
    timezone: Mapped[str] = mapped_column(String(50), nullable=False, default="UTC")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
