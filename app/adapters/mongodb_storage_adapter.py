"""MongoDB storage adapter for the Document Generator service."""
import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from bson.errors import InvalidId
from pymongo import MongoClient
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from app.config.db_config import MongoDbConfig, load_mongodb_config  # pylint: disable=import-error

MONGO_READ_FAILED_LITERAL = "MongoDB read failed"
MONGO_WRITE_FAILED_LITERAL = "MongoDB write failed"


class MongoStorageError(RuntimeError):
    """Raised when a MongoDB storage operation fails."""


class MongoDbStorageAdapter:
    """Adapter for MongoDB CRUD operations on uploads, content, and generated documents."""

    def __init__(self, config: MongoDbConfig | None = None) -> None:
        """Initialize the MongoDB client and collection references."""
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
        """Retrieve extracted content by content_id and version; returns None if not found."""
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
            raise MongoStorageError(MONGO_READ_FAILED_LITERAL) from exc

        if found is None:
            return None

        found["_id"] = str(found["_id"])
        return found

    def get_upload(self, upload_id: str) -> dict[str, Any] | None:
        """Retrieve an upload record by ID; returns None if not found."""
        try:
            object_id = ObjectId(upload_id)
        except (InvalidId, TypeError) as exc:
            raise MongoStorageError("Invalid upload_id format") from exc

        try:
            found = self.uploads_collection.find_one({"_id": object_id})
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_get_upload_failed upload_id=%s", upload_id)
            raise MongoStorageError(MONGO_READ_FAILED_LITERAL) from exc

        if found is None:
            return None

        found["_id"] = str(found["_id"])
        return found

    def get_generated_document(self, content_id: str, version: int) -> dict[str, Any] | None:
        """Retrieve a generated document record by content_id and version."""
        try:
            found = self.generated_documents_collection.find_one(
                {"content_id": content_id, "version": version}
            )
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_get_generated_document_failed content_id=%s version=%s",
                content_id,
                version,
            )
            raise MongoStorageError(MONGO_READ_FAILED_LITERAL) from exc

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
        """Insert or update a generated document record; returns the document ID."""
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
                "mongo_upsert_generated_document_failed content_id=%s version=%s",
                content_id,
                version,
            )
            raise MongoStorageError(MONGO_WRITE_FAILED_LITERAL) from exc

        if doc is None:
            raise MongoStorageError(MONGO_WRITE_FAILED_LITERAL)

        doc_id = str(doc["_id"])
        self.logger.info(
            "mongo_upsert_generated_document_success content_id=%s version=%s"
            " generated_document_id=%s",
            content_id,
            version,
            doc_id,
        )
        return doc_id

    def list_generated_documents(
        self,
        *,
        content_id: str | None = None,
        version: int | None = None,
        extension: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return a paginated list of generated documents and the total count."""
        query: dict[str, Any] = {}
        if content_id is not None:
            query["content_id"] = content_id
        if version is not None:
            query["version"] = version
        if extension is not None:
            query["extension"] = extension
        try:
            total = self.generated_documents_collection.count_documents(query)
            cursor = (
                self.generated_documents_collection
                .find(query)
                .sort("created_at", -1)
                .skip(offset)
                .limit(limit)
            )
            docs = list(cursor)
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_list_generated_documents_failed query=%s", query)
            raise MongoStorageError(MONGO_READ_FAILED_LITERAL) from exc
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return docs, total

    def list_generated_by_content_id(self, content_id: str) -> list[dict[str, Any]]:
        """Return all generated documents for a given content_id, newest first."""
        try:
            cursor = (
                self.generated_documents_collection
                .find({"content_id": content_id})
                .sort("created_at", -1)
            )
            docs = list(cursor)
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_list_generated_by_content_id_failed content_id=%s", content_id)
            raise MongoStorageError(MONGO_READ_FAILED_LITERAL) from exc
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return docs

    def get_generated_document_by_id(self, doc_id: str) -> dict[str, Any] | None:
        """Retrieve a generated document record by its MongoDB ObjectId string."""
        try:
            object_id = ObjectId(doc_id)
        except (InvalidId, TypeError) as exc:
            raise MongoStorageError(
                "Invalid generated document id format") from exc
        try:
            found = self.generated_documents_collection.find_one(
                {"_id": object_id})
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_get_generated_document_by_id_failed doc_id=%s", doc_id)
            raise MongoStorageError(MONGO_READ_FAILED_LITERAL) from exc
        if found is None:
            return None
        found["_id"] = str(found["_id"])
        return found

    def delete_generated_document(self, doc_id: str) -> dict[str, Any] | None:
        """Delete a generated document by ID; returns the deleted document or None."""
        try:
            object_id = ObjectId(doc_id)
        except (InvalidId, TypeError) as exc:
            raise MongoStorageError(
                "Invalid generated document id format") from exc
        try:
            found = self.generated_documents_collection.find_one_and_delete(
                {"_id": object_id}
            )
        except PyMongoError as exc:
            self.logger.exception(
                "mongo_delete_generated_document_failed doc_id=%s", doc_id)
            raise MongoStorageError(MONGO_WRITE_FAILED_LITERAL) from exc
        if found is not None:
            found["_id"] = str(found["_id"])
        return found

    def check_connection(self) -> bool:
        """Return True when MongoDB is reachable."""
        try:
            self.client.admin.command("ping")
            return True
        except PyMongoError:
            return False

    def close(self) -> None:
        """Close the underlying MongoDB client connection."""
        self.client.close()
