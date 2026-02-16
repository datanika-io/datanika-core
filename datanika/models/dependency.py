import enum

from sqlalchemy import BigInteger, Enum
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin


class NodeType(enum.StrEnum):
    PIPELINE = "pipeline"
    TRANSFORMATION = "transformation"


class Dependency(Base, TenantMixin, TimestampMixin):
    __tablename__ = "dependencies"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    upstream_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    upstream_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    downstream_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    downstream_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
