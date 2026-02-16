import enum

from sqlalchemy import Enum, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from etlfabric.models.base import Base, TenantMixin, TimestampMixin


class Materialization(enum.StrEnum):
    VIEW = "view"
    TABLE = "table"
    INCREMENTAL = "incremental"
    EPHEMERAL = "ephemeral"
    SNAPSHOT = "snapshot"


class Transformation(Base, TenantMixin, TimestampMixin):
    __tablename__ = "transformations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    sql_body: Mapped[str] = mapped_column(Text, nullable=False)
    materialization: Mapped[Materialization] = mapped_column(
        Enum(Materialization, native_enum=False, length=20),
        nullable=False,
        default=Materialization.VIEW,
    )
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False, default="staging")
    tests_config: Mapped[dict] = mapped_column(JSON, default=dict)
