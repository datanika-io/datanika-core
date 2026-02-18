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

    # File uploads
    file_uploads_dir: str = "./uploaded_files"

    # App
    app_name: str = "Datanika"
    debug: bool = False


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
