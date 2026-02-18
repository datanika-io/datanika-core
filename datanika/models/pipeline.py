import enum

from sqlalchemy import BigInteger, Boolean, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import JSON

from datanika.models.base import Base, TenantMixin, TimestampMixin


class DbtCommand(enum.StrEnum):
    BUILD = "build"
    RUN = "run"
    TEST = "test"
    SEED = "seed"
    SNAPSHOT = "snapshot"
    COMPILE = "compile"


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
    destination_connection_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("connections.id"), nullable=False
    )
    command: Mapped[DbtCommand] = mapped_column(
        Enum(DbtCommand, native_enum=False, length=20),
        nullable=False,
        default=DbtCommand.RUN,
    )
    full_refresh: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    models: Mapped[list] = mapped_column(JSON, default=list)
    custom_selector: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[PipelineStatus] = mapped_column(
        Enum(PipelineStatus, native_enum=False, length=20),
        nullable=False,
        default=PipelineStatus.DRAFT,
    )

    destination_connection = relationship(
        "Connection", foreign_keys=[destination_connection_id], lazy="joined"
    )
