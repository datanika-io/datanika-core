import enum
import uuid

from sqlalchemy import Enum
from sqlalchemy.orm import Mapped, mapped_column

from etlfabric.models.base import Base, TenantMixin, TimestampMixin, UUIDType


class NodeType(str, enum.Enum):
    PIPELINE = "pipeline"
    TRANSFORMATION = "transformation"


class Dependency(Base, TenantMixin, TimestampMixin):
    __tablename__ = "dependencies"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(), primary_key=True, default=uuid.uuid4
    )
    upstream_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    upstream_id: Mapped[uuid.UUID] = mapped_column(UUIDType(), nullable=False)
    downstream_type: Mapped[NodeType] = mapped_column(
        Enum(NodeType, native_enum=False, length=20), nullable=False
    )
    downstream_id: Mapped[uuid.UUID] = mapped_column(UUIDType(), nullable=False)
