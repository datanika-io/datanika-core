from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://etlfabric:etlfabric@localhost:5432/etlfabric"
    database_url_sync: str = "postgresql://etlfabric:etlfabric@localhost:5432/etlfabric"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Auth
    secret_key: str = "change-me-to-a-random-secret-key"
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    # Encryption
    credential_encryption_key: str = "change-me-generate-with-fernet"

    # OAuth
    google_client_id: str = ""
    google_client_secret: str = ""
    github_client_id: str = ""
    github_client_secret: str = ""
    oauth_redirect_base_url: str = "http://localhost:8000"
    frontend_url: str = "http://localhost:3000"

    # dbt
    dbt_projects_dir: str = "./dbt_projects"

    # App
    app_name: str = "ETL Fabric"
    debug: bool = True


settings = Settings()
