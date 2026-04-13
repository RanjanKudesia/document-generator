import logging
import mimetypes
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.client import BaseClient
from botocore.exceptions import ClientError

from app.config.storage_config import S3StorageConfig, load_s3_storage_config


class S3StorageError(RuntimeError):
    pass


class S3StorageAdapter:
    def __init__(self, config: S3StorageConfig | None = None) -> None:
        self.logger = logging.getLogger(__name__)
        self.config = config or load_s3_storage_config()

        cfg = Config(
            signature_version=self.config.signature_version,
            s3={"addressing_style": self.config.addressing_style},
        )

        self.client: BaseClient = boto3.client(
            "s3",
            endpoint_url=self.config.endpoint_url,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            aws_session_token=self.config.session_token,
            region_name=self.config.region,
            config=cfg,
        )

    def build_key(self, *parts: str) -> str:
        segments = [self.config.key_prefix]
        for part in parts:
            cleaned = (part or "").strip("/")
            if cleaned:
                segments.append(cleaned)
        return "/".join(segments)

    def upload_bytes(self, data: bytes, key: str, content_type: str | None = None) -> str:
        params: dict[str, object] = {
            "Bucket": self.config.bucket_name,
            "Key": key,
            "Body": data,
        }
        if content_type:
            params["ContentType"] = content_type

        try:
            self.client.put_object(**params)
            self.logger.info(
                "s3_upload_success key=%s bytes=%s", key, len(data))
        except ClientError as exc:
            self.logger.exception("s3_upload_failed key=%s", key)
            raise S3StorageError("S3 upload failed") from exc
        return key

    def upload_file(self, file_path: Path | str, key: str, content_type: str | None = None) -> str:
        path = Path(file_path)
        guessed_type = content_type or mimetypes.guess_type(str(path))[0]
        return self.upload_bytes(path.read_bytes(), key, guessed_type)

    def download_bytes(self, key: str) -> bytes:
        try:
            response = self.client.get_object(
                Bucket=self.config.bucket_name, Key=key)
            body = response.get("Body")
            if body is None:
                raise S3StorageError("S3 response body missing")
            data = body.read()
            self.logger.info(
                "s3_download_success key=%s bytes=%s", key, len(data))
            return data
        except ClientError as exc:
            self.logger.exception("s3_download_failed key=%s", key)
            raise S3StorageError(f"S3 download failed for key: {key}") from exc

    def generate_presigned_download_url(self, key: str, expires_in_seconds: int = 3600) -> str:
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.config.bucket_name, "Key": key},
                ExpiresIn=expires_in_seconds,
            )
            self.logger.info(
                "s3_presigned_url_generated key=%s expires_in_seconds=%s",
                key,
                expires_in_seconds,
            )
            return url
        except ClientError as exc:
            self.logger.exception("s3_presigned_url_failed key=%s", key)
            raise S3StorageError(
                "Failed to generate S3 presigned URL") from exc

    def object_exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.config.bucket_name, Key=key)
            return True
        except ClientError:
            return False

    def check_bucket_access(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.config.bucket_name)
            return True
        except ClientError:
            return False
