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

    # Load shedding in front of the limiter (#774). These bound what an
    # *unauthenticated* caller can cost us: before this, a request with an
    # invalid key was never counted at all, so 40 of 40 measured requests each
    # bought a session checkout, a sha256 and an indexed SELECT for free.
    # Failures only — a valid key never touches these counters, so a legitimate
    # caller behind a shared address is unaffected until that address itself is
    # failing at this rate. Set either to 0 to disable that bucket.
    api_auth_failure_limit: int = 10  # per presented credential, per window
    api_auth_failure_ip_limit: int = 60  # per client address, per window
    api_auth_failure_window_seconds: int = 60

    # How long a resolved per-org rate limit may be reused before the plan is
    # read again. 89% of the measured cost of enforcing a rate limit was the DB
    # session opened to discover a value that changes about monthly
    # (4.20 ms of 4.74 ms, on the production box). 0 disables the cache.
    api_plan_limit_cache_seconds: int = 60

    # App
    app_name: str = "Datanika"
    debug: bool = False

    # Edition: "core" (open-source) or "cloud" (SaaS with billing)
    datanika_edition: str = "core"

    # V2 pricing pivot — dual-mode (ETL/ELT) UX surface. Gated until Engineering's
    # P1 plumbing (bytes quota hook, IR mode resolver) ships; safe to enable once
    # SPEC_DUAL_MODE_UX v3 components are live on both core and cloud.
    datanika_dual_mode_ux_enabled: bool = False

    # SPEC_LOCAL_FILE_CONNECTIONS D4 (core#969). May a connection store a path
    # to a **local filesystem** location — a sqlite/duckdb file, or a
    # csv/json/parquet directory?
    #
    # 🚨 **Default True, and that is not timidity.** The feature was only ever
    # for self-hosters, who are the people a path on their own disk means
    # something to, and defaulting to False would break every existing local
    # deployment on upgrade. Production sets it to `false` in `.env.docker`.
    #
    # ⚠️ **Not gated on `datanika_edition`.** The property that matters is *"is
    # this deployment multi-tenant?"* — a deployment fact, not an edition.
    # `DATANIKA_EDITION=cloud` gates billing, and a self-hoster running the cloud
    # plugin is a shape we support; that person must keep their local paths.
    #
    # On a hosted box a path the user types is not a path on their machine: it
    # names a location inside our container, on infrastructure shared with every
    # other tenant. See core#969 — production connection id=14 points at a
    # directory inside the image that no code path writes and no backup could
    # regenerate.
    datanika_allow_local_file_paths: bool = True


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
