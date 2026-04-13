import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import MongoClient
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from app.config.db_config import MongoDbConfig, load_mongodb_config


class MongoStorageError(RuntimeError):
    pass


class MongoDbStorageAdapter:
    def __init__(self, config: MongoDbConfig | None = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.config = config or load_mongodb_config()

        self.client = MongoClient(
            self.config.mongo_uri,
            serverSelectionTimeoutMS=5000,
        )
        db = self.client[self.config.database_name]
        self.uploads_collection = db[self.config.uploads_collection_name]
        self.content_collection = db[self.config.content_collection_name]
        self.generated_documents_collection = db[self.config.generated_documents_collection_name]

    def get_content(self, content_id: str, version: int) -> dict[str, Any] | None:
        try:
            object_id = ObjectId(content_id)
        except (InvalidId, TypeError) as exc:
            raise MongoStorageError("Invalid content_id format") from exc

        try:
            found = self.content_collection.find_one(
                {"_id": object_id, "version": version})
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_get_content_failed content_id=%s version=%s", content_id, version)
            raise MongoStorageError("MongoDB read failed") from exc

        if found is None:
            return None

        found["_id"] = str(found["_id"])
        return found

    def get_upload(self, upload_id: str) -> dict[str, Any] | None:
        try:
            object_id = ObjectId(upload_id)
        except (InvalidId, TypeError) as exc:
            raise MongoStorageError("Invalid upload_id format") from exc

        try:
            found = self.uploads_collection.find_one({"_id": object_id})
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_get_upload_failed upload_id=%s", upload_id)
            raise MongoStorageError("MongoDB read failed") from exc

        if found is None:
            return None

        found["_id"] = str(found["_id"])
        return found

    def get_generated_document(self, content_id: str, version: int) -> dict[str, Any] | None:
        try:
            found = self.generated_documents_collection.find_one(
                {"content_id": content_id, "version": version}
            )
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_get_generated_document_failed content_id=%s version=%s", content_id, version)
            raise MongoStorageError("MongoDB read failed") from exc

        if found is None:
            return None

        found["_id"] = str(found["_id"])
        return found

    def upsert_generated_document(
        self,
        *,
        content_id: str,
        version: int,
        file_name: str,
        extension: str,
        output_file_s3_key: str,
        source_content_updated_at: str | None,
    ) -> str:
        now = datetime.now(timezone.utc)
        try:
            doc = self.generated_documents_collection.find_one_and_update(
                {"content_id": content_id, "version": version},
                {
                    "$set": {
                        "file_name": file_name,
                        "extension": extension,
                        "output_file_s3_key": output_file_s3_key,
                        "source_content_updated_at": source_content_updated_at,
                        "updated_at": now,
                    },
                    "$setOnInsert": {
                        "content_id": content_id,
                        "version": version,
                        "created_at": now,
                    },
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_upsert_generated_document_failed content_id=%s version=%s", content_id, version)
            raise MongoStorageError("MongoDB write failed") from exc

        if doc is None:
            raise MongoStorageError("MongoDB write failed")

        doc_id = str(doc["_id"])
        self.logger.info(
            "mongo_upsert_generated_document_success content_id=%s version=%s generated_document_id=%s",
            content_id,
            version,
            doc_id,
        )
        return doc_id

    def check_connection(self) -> bool:
        try:
            self.client.admin.command("ping")
            return True
        except PyMongoError:
            return False
