import enum

from sqlalchemy import BigInteger, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from etlfabric.models.base import Base, TenantMixin, TimestampMixin


class PipelineStatus(enum.StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    PAUSED = "paused"
    ERROR = "error"


class Pipeline(Base, TenantMixin, TimestampMixin):
    __tablename__ = "pipelines"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    source_connection_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("connections.id"), nullable=False
    )
    destination_connection_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("connections.id"), nullable=False
    )
    dlt_config: Mapped[dict] = mapped_column(JSON, default=dict)
    status: Mapped[PipelineStatus] = mapped_column(
        Enum(PipelineStatus, native_enum=False, length=20),
        nullable=False,
        default=PipelineStatus.DRAFT,
    )

    source_connection = relationship(
        "Connection", foreign_keys=[source_connection_id], lazy="joined"
    )
    destination_connection = relationship(
        "Connection", foreign_keys=[destination_connection_id], lazy="joined"
    )
