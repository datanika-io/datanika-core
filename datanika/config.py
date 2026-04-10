import logging
import warnings

from pydantic_settings import BaseSettings, SettingsConfigDict

_INSECURE_DEFAULT_KEY = "XEOMryjw0MylWx2uNX_4c7xvPzl9T5dBxxhCUmsQc8A"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Database
    database_url: str = "postgresql+asyncpg://datanika:datanika@localhost:5432/datanika"
    database_url_sync: str = "postgresql://datanika:datanika@localhost:5432/datanika"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Auth
    secret_key: str = _INSECURE_DEFAULT_KEY
    access_token_expire_minutes: int = 15
    refresh_token_expire_days: int = 7

    # Encryption
    credential_encryption_key: str = _INSECURE_DEFAULT_KEY

    # OAuth
    google_client_id: str = ""
    google_client_secret: str = ""
    github_client_id: str = ""
    github_client_secret: str = ""
    oauth_redirect_base_url: str = "http://localhost:8000"
    frontend_url: str = "http://localhost:3000"

    # reCAPTCHA
    recaptcha_site_key: str = ""
    recaptcha_secret_key: str = ""

    # dbt
    dbt_projects_dir: str = "./dbt_projects"

    # dlt pipeline working directories (extract buffers, load packages)
    dlt_pipelines_dir: str = "./dlt_pipelines"

    # File uploads
    file_uploads_dir: str = "./uploaded_files"

    # Maintenance
    maintenance_dlt_max_age_hours: int = 24
    maintenance_dbt_max_age_hours: int = 48
    maintenance_run_retention_days: int = 90

    # Email / SMTP (disabled when smtp_host is empty)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_from_email: str = "noreply@datanika.io"
    smtp_from_name: str = "Datanika"
    smtp_use_tls: bool = True
    email_verification_required: bool = False

    # SSO
    sso_sp_entity_id: str = "datanika"

    # Database connection pool
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_timeout: int = 30
    db_pool_recycle: int = 1800  # 30 minutes

    # API rate limiting (requests per minute, per API key)
    api_rate_limit_rpm: int = 60
    api_rate_limit_burst: int = 10

    # App
    app_name: str = "Datanika"
    debug: bool = False

    # Edition: "core" (open-source) or "cloud" (SaaS with billing)
    datanika_edition: str = "core"


settings = Settings()

# Warn loudly when using default secret keys
_log = logging.getLogger(__name__)
if settings.secret_key == _INSECURE_DEFAULT_KEY:
    msg = (
        "SECRET_KEY is using the insecure default value. "
        "Set SECRET_KEY in your .env file for production."
    )
    _log.warning(msg)
    if not settings.debug:
        warnings.warn(msg, stacklevel=1)
if settings.credential_encryption_key == _INSECURE_DEFAULT_KEY:
    msg = (
        "CREDENTIAL_ENCRYPTION_KEY is using the insecure default value. "
        "Set CREDENTIAL_ENCRYPTION_KEY in your .env file for production."
    )
    _log.warning(msg)
    if not settings.debug:
        warnings.warn(msg, stacklevel=1)
