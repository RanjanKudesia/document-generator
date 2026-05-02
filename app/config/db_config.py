import os
import logging
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MongoDbConfig:
    mongo_uri: str
    database_name: str
    uploads_collection_name: str
    content_collection_name: str
    generated_documents_collection_name: str


def load_mongodb_config() -> MongoDbConfig:
    """Load MongoDB configuration from environment variables (.env file)."""
    base_dir = Path(__file__).resolve().parents[2]
    load_dotenv(base_dir / ".env")

    mongo_uri = _required_env("MONGODB_URI")
    if not (mongo_uri.startswith("mongodb://") or mongo_uri.startswith("mongodb+srv://")):
        mongo_uri = f"mongodb://{mongo_uri}"
        logger.debug("db_config_normalized_uri scheme=mongodb://")

    config = MongoDbConfig(
        mongo_uri=mongo_uri,
        database_name=os.getenv("MONGODB_DATABASE", "content_extractor"),
        uploads_collection_name=os.getenv(
            "MONGODB_UPLOADS_COLLECTION", "uploads"),
        content_collection_name=os.getenv(
            "MONGODB_CONTENT_COLLECTION", "content"),
        generated_documents_collection_name=os.getenv(
            "MONGODB_GENERATED_DOCUMENTS_COLLECTION", "generated_documents"
        ),
    )
    logger.debug(
        "db_config_loaded database=%s uploads_coll=%s content_coll=%s generated_coll=%s",
        config.database_name,
        config.uploads_collection_name,
        config.content_collection_name,
        config.generated_documents_collection_name,
    )
    return config


def _required_env(key: str) -> str:
    """Return the value of a required environment variable or raise ValueError."""
    value = os.getenv(key)
    if value is None or not value.strip():
        raise ValueError(f"Missing required environment variable: {key}")
    return value.strip()
