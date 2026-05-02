import base64
import logging
import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from fastapi import APIRouter, HTTPException, status

from app.adapters.mongodb_storage_adapter import MongoDbStorageAdapter, MongoStorageError
from app.adapters.s3_storage_adapter import S3StorageAdapter, S3StorageError
from app.pipelines.html_generation_pipeline import HtmlGenerationPipeline
from app.pipelines.markdown_generation_pipeline import MarkdownGenerationPipeline
from app.pipelines.pdf_generation_pipeline import PdfGenerationPipeline
from app.pipelines.ppt_generation_pipeline import PptGenerationPipeline
from app.pipelines.text_generation_pipeline import TextGenerationPipeline
from app.pipelines.docx_generation_pipeline import DocxGenerationPipeline
from app.schemas.document_generation_schema import (
    DocumentGenerationRequest,
    DocumentGenerationResponse,
    HealthResponse,
    ResolvedDocumentGenerationPayload,
)

_FILENAME_REGEX = re.compile(r"^[A-Za-z0-9_-]+$")
_UUID_STEM_REGEX = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_SUPPORTED_EXTENSIONS = {"docx", "dox", "pdf",
                         "md", "txt", "html", "htm", "ppt", "pptx"}

router = APIRouter()
logger = logging.getLogger(__name__)

_PDF_MIME = "application/pdf"
_DOCX_MIME = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
_MD_MIME = "text/markdown; charset=utf-8"
_TXT_MIME = "text/plain; charset=utf-8"
_HTML_MIME = "text/html; charset=utf-8"
_PPTX_MIME = "application/vnd.openxmlformats-officedocument.presentationml.presentation"
_PRESIGNED_URL_EXPIRY_SECONDS = 3600


@router.get("/health", tags=["health"])
def get_health() -> HealthResponse:
    return HealthResponse(status="ok", service="document-generator")


@router.get("/generate", tags=["generate"])
def generate_document(content_id: str, version: int) -> DocumentGenerationResponse:
    """Build or reuse a generated document for the given content version."""
    logger.info("generate_request_received content_id=%s version=%s",
                content_id, version)
    payload = DocumentGenerationRequest(content_id=content_id, version=version)
    mongo_adapter = _get_mongo_adapter()
    s3_adapter = _get_s3_adapter()
    resolved_payload = _resolve_payload(payload)

    ext = (resolved_payload.extension or "docx").lower().lstrip(".")

    if ext not in _SUPPORTED_EXTENSIONS:
        logger.warning(
            "generate_request_unsupported_extension extension=%s", ext)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported extension '{ext}'. Supported: {sorted(_SUPPORTED_EXTENSIONS)}",
        )

    output_ext = _normalize_extension(ext)
    logger.info(
        "generate_request_resolved_extension input=%s output=%s", ext, output_ext)
    file_stem = _derive_file_stem(resolved_payload)
    file_name = f"{file_stem}.{output_ext}"
    output_key = _build_generated_output_key(
        content_id=payload.content_id,
        version=payload.version,
        file_name=file_name,
        s3_adapter=s3_adapter,
    )

    if s3_adapter.object_exists(output_key):
        logger.info(
            "generate_cache_hit content_id=%s version=%s output_key=%s",
            payload.content_id,
            payload.version,
            output_key,
        )
        try:
            generated_document_id = mongo_adapter.upsert_generated_document(
                content_id=payload.content_id,
                version=payload.version,
                file_name=file_name,
                extension=output_ext,
                output_file_s3_key=output_key,
                source_content_updated_at=resolved_payload.source_content_updated_at,
            )
        except MongoStorageError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(exc),
            ) from exc

        presigned_url = s3_adapter.generate_presigned_download_url(
            output_key,
            expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
        )
        expires_at = datetime.now(
            timezone.utc) + timedelta(seconds=_PRESIGNED_URL_EXPIRY_SECONDS)
        return DocumentGenerationResponse(
            id=generated_document_id,
            file_name=file_name,
            output_file_s3_key=output_key,
            download_url=presigned_url,
            url_expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
            url_expires_at=expires_at.isoformat(),
            extension=output_ext,
        )

    if output_ext == "pdf":
        file_bytes = _run_pdf(resolved_payload, file_name)
    elif output_ext == "pptx":
        file_bytes = _run_pptx(resolved_payload, file_name)
    elif output_ext == "html":
        file_bytes = _run_html(resolved_payload, file_name)
    elif output_ext == "md":
        file_bytes = _run_md(resolved_payload, file_name)
    elif output_ext == "txt":
        file_bytes = _run_txt(resolved_payload, file_name)
    else:
        file_bytes = _run_docx(resolved_payload, file_name)

    content_type = _content_type_for_extension(output_ext)

    try:
        s3_adapter.upload_bytes(
            file_bytes, key=output_key, content_type=content_type)
        logger.info(
            "generate_output_uploaded content_id=%s version=%s output_key=%s",
            payload.content_id,
            payload.version,
            output_key,
        )
        generated_document_id = mongo_adapter.upsert_generated_document(
            content_id=payload.content_id,
            version=payload.version,
            file_name=file_name,
            extension=output_ext,
            output_file_s3_key=output_key,
            source_content_updated_at=resolved_payload.source_content_updated_at,
        )
        presigned_url = s3_adapter.generate_presigned_download_url(
            output_key, expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS
        )
    except (S3StorageError, MongoStorageError) as exc:
        logger.exception(
            "generate_request_failed content_id=%s version=%s error=%s",
            payload.content_id,
            payload.version,
            str(exc),
        )
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc

    expires_at = datetime.now(timezone.utc) + \
        timedelta(seconds=_PRESIGNED_URL_EXPIRY_SECONDS)

    response = DocumentGenerationResponse(
        id=generated_document_id,
        file_name=file_name,
        output_file_s3_key=output_key,
        download_url=presigned_url,
        url_expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
        url_expires_at=expires_at.isoformat(),
        extension=output_ext,
    )
    logger.info(
        "generate_request_completed content_id=%s version=%s generated_id=%s",
        payload.content_id,
        payload.version,
        generated_document_id,
    )
    return response


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _derive_file_stem(payload: ResolvedDocumentGenerationPayload) -> str:
    candidate = payload.original_filename or payload.stored_filename or ""
    if candidate:
        stem = Path(candidate).stem
        cleaned_stem = re.sub(r"[^A-Za-z0-9_-]", "_", stem).strip("_")[:100]
        if cleaned_stem and not _UUID_STEM_REGEX.fullmatch(cleaned_stem):
            return cleaned_stem

    if payload.extracted_data is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No extracted_data found for the requested content_id and version.",
        )

    return "document"


def _build_generated_output_key(
    *,
    content_id: str,
    version: int,
    file_name: str,
    s3_adapter: S3StorageAdapter,
) -> str:
    return s3_adapter.build_key(
        "generated",
        content_id,
        f"version-{version}",
        file_name,
    )


@lru_cache(maxsize=1)
def _get_s3_adapter() -> S3StorageAdapter:
    return S3StorageAdapter()


@lru_cache(maxsize=1)
def _get_mongo_adapter() -> MongoDbStorageAdapter:
    return MongoDbStorageAdapter()


def _normalize_extension(extension: str) -> str:
    if extension == "dox":
        return "docx"
    if extension == "htm":
        return "html"
    if extension == "ppt":
        return "pptx"
    return extension


def _content_type_for_extension(extension: str) -> str:
    if extension == "pdf":
        return _PDF_MIME
    if extension == "pptx":
        return _PPTX_MIME
    if extension == "html":
        return _HTML_MIME
    if extension == "md":
        return _MD_MIME
    if extension == "txt":
        return _TXT_MIME
    return _DOCX_MIME


def _resolve_payload(payload: DocumentGenerationRequest) -> ResolvedDocumentGenerationPayload:
    mongo_adapter = _get_mongo_adapter()
    try:
        stored_content = mongo_adapter.get_content(
            content_id=payload.content_id,
            version=payload.version,
        )
    except MongoStorageError as exc:
        detail = str(exc)
        if "Invalid content_id" in detail:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=detail) from exc

    if stored_content is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No extracted content found for content_id={payload.content_id} "
                f"and version={payload.version}"
            ),
        )

    upload_metadata = None
    upload_id = stored_content.get("upload_id")
    if isinstance(upload_id, str) and upload_id:
        try:
            upload_metadata = mongo_adapter.get_upload(upload_id)
        except MongoStorageError:
            upload_metadata = None

    merged = payload.model_dump(mode="python")
    data_s3_key = stored_content.get("data_s3_key")
    if data_s3_key:
        import json as _json
        raw_bytes = _get_s3_adapter().download_bytes(data_s3_key)
        merged["extracted_data"] = _json.loads(raw_bytes)
    else:
        # Backward-compat: older records stored the payload inline.
        merged["extracted_data"] = stored_content.get("data")
    source_content_updated_at = stored_content.get("updated_at")
    normalized_source_updated_at = None
    if source_content_updated_at is not None:
        if hasattr(source_content_updated_at, "isoformat"):
            normalized_source_updated_at = source_content_updated_at.isoformat()
        else:
            normalized_source_updated_at = str(source_content_updated_at)
    merged["source_content_updated_at"] = normalized_source_updated_at
    if upload_metadata is not None:
        merged["original_filename"] = upload_metadata.get("original_filename")
        merged["stored_filename"] = upload_metadata.get("stored_filename")
        merged["extension"] = upload_metadata.get("extension")

    extracted_data = merged.get("extracted_data")
    if isinstance(extracted_data, dict):
        try:
            _hydrate_media_from_s3(extracted_data, _get_s3_adapter())
        except S3StorageError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(exc),
            ) from exc

    return ResolvedDocumentGenerationPayload.model_validate(merged)


def _hydrate_media_from_s3(node: object, s3_adapter: S3StorageAdapter) -> None:
    if isinstance(node, dict):
        s3_key = node.get("s3_key")
        has_inline_blob = bool(node.get("base64") or node.get("base64_data"))
        if isinstance(s3_key, str) and s3_key and not has_inline_blob:
            blob = s3_adapter.download_bytes(s3_key)
            encoded = base64.b64encode(blob).decode("ascii")
            node["base64"] = encoded
            node["base64_data"] = encoded

        for value in node.values():
            _hydrate_media_from_s3(value, s3_adapter)
        return

    if isinstance(node, list):
        for item in node:
            _hydrate_media_from_s3(item, s3_adapter)


def _run_docx(payload: DocumentGenerationRequest, file_name: str) -> bytes:
    pipeline = DocxGenerationPipeline()
    return pipeline.run(payload=payload, file_name=file_name)


def _run_pdf(payload: DocumentGenerationRequest, file_name: str) -> bytes:
    try:
        pipeline = PdfGenerationPipeline()
        return pipeline.run(payload=payload, file_name=file_name)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(exc),
        ) from exc


def _run_md(payload: DocumentGenerationRequest, file_name: str) -> bytes:
    pipeline = MarkdownGenerationPipeline()
    return pipeline.run(payload=payload, file_name=file_name)


def _run_txt(payload: DocumentGenerationRequest, file_name: str) -> bytes:
    pipeline = TextGenerationPipeline()
    return pipeline.run(payload=payload, file_name=file_name)


def _run_html(payload: DocumentGenerationRequest, file_name: str) -> bytes:
    pipeline = HtmlGenerationPipeline()
    return pipeline.run(payload=payload, file_name=file_name)


def _run_pptx(payload: DocumentGenerationRequest, file_name: str) -> bytes:
    try:
        pipeline = PptGenerationPipeline()
    except ModuleNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="PPT generation dependency missing: install 'python-pptx'.",
        ) from exc
    return pipeline.run(payload=payload, file_name=file_name)
