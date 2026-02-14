import enum
import uuid

from sqlalchemy import Enum, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from etlfabric.models.base import Base, TenantMixin, TimestampMixin, UUIDType


class ConnectionType(str, enum.Enum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    MSSQL = "mssql"
    SQLITE = "sqlite"
    REST_API = "rest_api"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    S3 = "s3"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"


class ConnectionDirection(str, enum.Enum):
    SOURCE = "source"
    DESTINATION = "destination"
    BOTH = "both"


class Connection(Base, TenantMixin, TimestampMixin):
    __tablename__ = "connections"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    connection_type: Mapped[ConnectionType] = mapped_column(
        Enum(ConnectionType, native_enum=False, length=30), nullable=False
    )
    direction: Mapped[ConnectionDirection] = mapped_column(
        Enum(ConnectionDirection, native_enum=False, length=20), nullable=False
    )
    config_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
