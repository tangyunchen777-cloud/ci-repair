from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: SecretStr | None = None
    database_ssl: bool = True
    database_ssl_ca: Path | None = None
    aws_region: str | None = None
    aws_endpoint_url: str | None = None
    sqs_queue_url: str | None = None
    s3_bucket: str | None = None
    github_token: SecretStr | None = None
    github_api_version: str = "2026-03-10"
    github_connect_timeout: float = Field(default=10, gt=0, le=30)
    github_read_timeout: float = Field(default=30, gt=0, le=60)
    max_log_bytes: int = Field(default=25 * 1024 * 1024, gt=0, le=100 * 1024 * 1024)
    operation_timeout_seconds: int = Field(default=180, ge=1, le=210)
    worker_lease_seconds: int = Field(default=240, ge=2, le=270)
    enqueue_lease_seconds: int = Field(default=120, ge=60, le=270)

    @model_validator(mode="after")
    def lease_outlasts_operation(self):
        if self.worker_lease_seconds <= self.operation_timeout_seconds:
            raise ValueError("Worker lease must outlast the operation deadline")
        return self

    def require(self, *names: str) -> None:
        if any(not getattr(self, name) for name in names):
            raise RuntimeError("Missing required service configuration")


@lru_cache
def get_settings() -> Settings:
    return Settings()
