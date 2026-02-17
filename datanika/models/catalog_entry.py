import enum

from sqlalchemy import BigInteger, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from datanika.models.base import Base, TenantMixin, TimestampMixin
from datanika.models.dependency import NodeType


class CatalogEntryType(enum.StrEnum):
    SOURCE_TABLE = "source_table"
    DBT_MODEL = "dbt_model"


class CatalogEntry(Base, TenantMixin, TimestampMixin):
    __tablename__ = "catalog_entries"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    entry_type: Mapped[CatalogEntryType] = mapped_column(
        Enum(CatalogEntryType, native_enum=False, length=20), nullable=False
    )
    origin_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    origin_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_name: Mapped[str] = mapped_column(String(255), nullable=False)
    dataset_name: Mapped[str] = mapped_column(String(255), nullable=False)
    connection_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("connections.id"), nullable=True
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    columns: Mapped[list | None] = mapped_column(JSON, default=list)
    dbt_config: Mapped[dict | None] = mapped_column(JSON, default=dict)
