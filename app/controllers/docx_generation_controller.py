from datetime import datetime, timedelta, timezone
from uuid import uuid4

from app.adapters.s3_storage_adapter import S3StorageAdapter
from app.pipelines.docx_generation_pipeline import DocxGenerationPipeline
from app.schemas.document_generation_schema import (
    DocumentGenerationRequest,
    DocumentGenerationResponse,
)


class DocxGenerationController:
    def __init__(self, s3_adapter: S3StorageAdapter | None = None) -> None:
        self.pipeline = DocxGenerationPipeline()
        self.s3_adapter = s3_adapter or S3StorageAdapter()

    def execute(self, payload: DocumentGenerationRequest) -> DocumentGenerationResponse:
        file_stem = str(uuid4())
        file_name = f"{file_stem}.docx"
        file_bytes = self.pipeline.run(payload=payload, file_name=file_name)

        output_key = self.s3_adapter.build_key(
            "generated", file_stem, file_name)
        self.s3_adapter.upload_bytes(
            file_bytes,
            key=output_key,
            content_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

        expires_in_seconds = 3600
        download_url = self.s3_adapter.generate_presigned_download_url(
            output_key,
            expires_in_seconds=expires_in_seconds,
        )
        expires_at = datetime.now(timezone.utc) + \
            timedelta(seconds=expires_in_seconds)

        return DocumentGenerationResponse(
            id=file_stem,
            file_name=file_name,
            output_file_s3_key=output_key,
            download_url=download_url,
            url_expires_in_seconds=expires_in_seconds,
            url_expires_at=expires_at.isoformat(),
            extension="docx",
        )
