"""Singleton SchedulerIntegrationService instance for the app."""

from datanika.config import settings
from datanika.services.scheduler_integration import SchedulerIntegrationService

scheduler_integration = SchedulerIntegrationService(settings.database_url_sync)
