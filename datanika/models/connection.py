import enum

from sqlalchemy import Enum, String, Text
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from datanika.models.base import Base, TenantMixin, TimestampMixin


class ConnectionType(enum.StrEnum):
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


class ConnectionDirection(enum.StrEnum):
    SOURCE = "source"
    DESTINATION = "destination"
    BOTH = "both"


class Connection(Base, TenantMixin, TimestampMixin):
    __tablename__ = "connections"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    connection_type: Mapped[ConnectionType] = mapped_column(
        Enum(ConnectionType, native_enum=False, length=30), nullable=False
    )
    direction: Mapped[ConnectionDirection] = mapped_column(
        Enum(ConnectionDirection, native_enum=False, length=20), nullable=False
    )
    config_encrypted: Mapped[str] = mapped_column(Text, nullable=False)
    freshness_config: Mapped[dict | None] = mapped_column(JSON, nullable=True, default=None)
