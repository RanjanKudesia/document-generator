"""S3 storage configuration loader for the Document Generator service."""
import os
import logging
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class S3StorageConfig:
    """Immutable configuration for the S3-compatible storage client."""
    bucket_name: str
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str
    session_token: str | None
    signature_version: str
    addressing_style: str
    key_prefix: str


def load_s3_storage_config() -> S3StorageConfig:
    """Load S3 storage configuration from environment variables (.env file)."""
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(base_dir / ".env")

    config = S3StorageConfig(
        bucket_name=_required_env("S3_BUCKET_NAME"),
        endpoint_url=_required_env("S3_ENDPOINT_URL"),
        access_key=_required_env("S3_ACCESS_KEY_ID"),
        secret_key=_required_env("S3_SECRET_ACCESS_KEY"),
        region=os.getenv("S3_REGION", "us-east-1"),
        session_token=os.getenv("S3_SESSION_TOKEN") or None,
        signature_version=os.getenv("S3_SIGNATURE_VERSION", "s3v4"),
        addressing_style=os.getenv("S3_ADDRESSING_STYLE", "path"),
        key_prefix=(os.getenv("S3_KEY_PREFIX", "document-playground")
                    or "document-playground").strip("/"),
    )
    logger.debug(
        "s3_config_loaded bucket=%s endpoint=%s region=%s key_prefix=%s",
        config.bucket_name, config.endpoint_url, config.region, config.key_prefix,
    )
    return config


def _required_env(key: str) -> str:
    """Return the value of a required environment variable or raise ValueError."""
    value = os.getenv(key)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {key}")
    return value.strip()
