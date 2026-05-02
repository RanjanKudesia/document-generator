"""Route handlers for the Document Generator API."""
import base64
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Body, HTTPException, Query, status

from app.adapters.mongodb_storage_adapter import MongoDbStorageAdapter, MongoStorageError
from app.adapters.s3_storage_adapter import S3StorageAdapter, S3StorageError
from app.pipelines.html_generation_pipeline import HtmlGenerationPipeline
from app.pipelines.markdown_generation_pipeline import MarkdownGenerationPipeline
from app.pipelines.pdf_generation_pipeline import PdfGenerationPipeline
from app.pipelines.ppt_generation_pipeline import PptGenerationPipeline
from app.pipelines.text_generation_pipeline import TextGenerationPipeline
from app.pipelines.docx_generation_pipeline import DocxGenerationPipeline
from app.schemas.document_generation_schema import (
    BatchGenerateItem,
    BatchGenerateResponse,
    BatchGenerateResult,
    DeleteGeneratedResponse,
    DependencyStatus,
    DocumentGenerationRequest,
    DocumentGenerationResponse,
    FreshDownloadUrlResponse,
    GeneratedDocumentListResponse,
    GeneratedDocumentRecord,
    GenerationCapabilitiesResponse,
    HealthResponse,
    HealthWithDependenciesResponse,
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


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        parsed = int(raw)
        return parsed if parsed > 0 else default
    except ValueError:
        return default


_MAX_EXTRACTED_JSON_BYTES = _env_int(
    "MAX_EXTRACTED_JSON_BYTES", 10 * 1024 * 1024)
_MAX_MEDIA_BYTES = _env_int("MAX_MEDIA_BYTES", 15 * 1024 * 1024)
_MAX_HYDRATION_DEPTH = _env_int("MAX_HYDRATION_DEPTH", 50)
_MAX_HYDRATION_NODES = _env_int("MAX_HYDRATION_NODES", 20000)


@router.get(
    "/health",
    tags=["health"],
    summary="Service health status",
    description="Returns service status for Document Generator.",
)
def get_health() -> HealthResponse:
    """Return basic service health status."""
    return HealthResponse(status="ok", service="document-generator")


@router.post(
    "/generate",
    tags=["generation"],
    summary="Generate document from extracted content",
    description=(
        "Generates (or reuses cached) output document bytes "
        "for the requested content_id and version, "
        "then returns generated file metadata and download details."
    ),
    response_description="Generated document metadata and download URL.",
    responses={
        400: {"description": "Invalid request input or unsupported output extension."},
        404: {"description": "Requested content/version not found in extractor storage."},
        502: {"description": "Dependency failure while reading/storing generated assets."},
    },
)
def generate_document(
    payload: Annotated[
        DocumentGenerationRequest,
        Body(
            ...,
            examples={
                "default": {
                    "summary": "Generate document for content version",
                    "value": {
                        "content_id": "69f64331423c9bfe1bf883a1",
                        "version": 0,
                    },
                }
            },
        ),
    ]
) -> DocumentGenerationResponse:
    """Build or reuse a generated document for the given content version."""
    return _generate_document_from_payload(payload)


@router.get(
    "/health/dependencies",
    tags=["health"],
    summary="Health check with dependency status",
    description="Returns reachability status for S3 and MongoDB.",
)
def get_health_dependencies() -> HealthWithDependenciesResponse:
    """Return health status including S3 and MongoDB reachability."""
    s3_ok = _get_s3_adapter().check_bucket_access()
    mongo_ok = _get_mongo_adapter().check_connection()
    overall = "ok" if (s3_ok and mongo_ok) else "degraded"
    return HealthWithDependenciesResponse(
        status=overall,
        service="document-generator",
        dependencies=DependencyStatus(s3=s3_ok, mongodb=mongo_ok),
    )


@router.get(
    "/generate/capabilities",
    tags=["generation"],
    summary="Supported extensions and service limits",
)
def get_capabilities() -> GenerationCapabilitiesResponse:
    """Return supported extensions and service configuration limits."""
    return GenerationCapabilitiesResponse(
        supported_extensions=sorted(_SUPPORTED_EXTENSIONS),
        max_json_bytes=_MAX_EXTRACTED_JSON_BYTES,
        max_media_bytes=_MAX_MEDIA_BYTES,
        max_hydration_depth=_MAX_HYDRATION_DEPTH,
        max_hydration_nodes=_MAX_HYDRATION_NODES,
        url_expiry_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
    )


@router.post(
    "/generate/batch",
    tags=["generation"],
    summary="Batch generate documents",
    description=(
        "Generate (or reuse cached) documents for multiple"
        " content_id/version combos."
    ),
)
def batch_generate(
    items: Annotated[
        list[BatchGenerateItem],
        Body(..., min_length=1, max_length=50),
    ]
) -> BatchGenerateResponse:
    """Generate documents for multiple content_id/version combinations."""
    results: list[BatchGenerateResult] = []
    for item in items:
        req = DocumentGenerationRequest(
            content_id=item.content_id,
            version=item.version,
        )
        try:
            doc_response = _generate_document_from_payload(req)
            results.append(BatchGenerateResult(
                content_id=item.content_id,
                version=item.version,
                extension=doc_response.extension,
                status="success",
                result=doc_response,
            ))
        except HTTPException as exc:
            results.append(BatchGenerateResult(
                content_id=item.content_id,
                version=item.version,
                extension=None,
                status="error",
                error=exc.detail,
            ))
    succeeded = sum(1 for r in results if r.status == "success")
    return BatchGenerateResponse(
        results=results,
        total=len(results),
        succeeded=succeeded,
        failed=len(results) - succeeded,
    )


@router.get(
    "/generated",
    tags=["generated"],
    summary="List generated document records",
    description="Paginated list of generated document records, optionally filtered.",
)
def list_generated(
    content_id: Annotated[str | None, Query(
        description="Filter by content_id.")] = None,
    version: Annotated[int | None, Query(
        ge=0, description="Filter by version.")] = None,
    extension: Annotated[str | None, Query(
        description="Filter by extension (e.g. pdf).")] = None,
    limit: Annotated[int, Query(
        ge=1, le=200, description="Max records to return.")] = 20,
    offset: Annotated[int, Query(
        ge=0, description="Number of records to skip.")] = 0,
) -> GeneratedDocumentListResponse:
    """Return a paginated list of generated document records."""
    mongo_adapter = _get_mongo_adapter()
    try:
        docs, total = mongo_adapter.list_generated_documents(
            content_id=content_id,
            version=version,
            extension=extension,
            limit=limit,
            offset=offset,
        )
    except MongoStorageError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    items = [_doc_to_record(d) for d in docs]
    return GeneratedDocumentListResponse(items=items, total=total, limit=limit, offset=offset)


@router.get(
    "/generated/by-content/{content_id}",
    tags=["generated"],
    summary="All generated documents for a content ID",
)
def list_generated_by_content(content_id: str) -> list[GeneratedDocumentRecord]:
    """Return all generated documents for a given content ID."""
    mongo_adapter = _get_mongo_adapter()
    try:
        docs = mongo_adapter.list_generated_by_content_id(content_id)
    except MongoStorageError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return [_doc_to_record(d) for d in docs]


@router.get(
    "/generated/{doc_id}/download-url",
    tags=["generated"],
    summary="Refresh presigned download URL",
    description="Returns a fresh presigned URL without regenerating the document.",
)
def get_fresh_download_url(doc_id: str) -> FreshDownloadUrlResponse:
    """Generate a fresh presigned download URL for an existing generated document."""
    mongo_adapter = _get_mongo_adapter()
    s3_adapter = _get_s3_adapter()
    doc = _get_generated_doc_or_404(doc_id, mongo_adapter)
    s3_key = doc.get("output_file_s3_key", "")
    try:
        url = s3_adapter.generate_presigned_download_url(
            s3_key, expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS
        )
    except S3StorageError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    expires_at = datetime.now(timezone.utc) + \
        timedelta(seconds=_PRESIGNED_URL_EXPIRY_SECONDS)
    return FreshDownloadUrlResponse(
        id=doc_id,
        download_url=url,
        url_expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
        url_expires_at=expires_at.isoformat(),
    )


@router.get(
    "/generated/{doc_id}",
    tags=["generated"],
    summary="Get a single generated document record by ID",
)
def get_generated_by_id(doc_id: str) -> GeneratedDocumentRecord:
    """Return a single generated document record by its MongoDB ID."""
    mongo_adapter = _get_mongo_adapter()
    doc = _get_generated_doc_or_404(doc_id, mongo_adapter)
    return _doc_to_record(doc)


@router.delete(
    "/generated/{doc_id}",
    tags=["generated"],
    summary="Delete a generated document",
    description="Removes the generated file from S3 and deletes the MongoDB record.",
)
def delete_generated(doc_id: str) -> DeleteGeneratedResponse:
    """Delete a generated document from S3 and MongoDB."""
    mongo_adapter = _get_mongo_adapter()
    s3_adapter = _get_s3_adapter()
    doc = _get_generated_doc_or_404(doc_id, mongo_adapter)
    s3_key = doc.get("output_file_s3_key", "")
    # Delete from S3 first; ignore if already gone
    try:
        s3_adapter.delete_key(s3_key)
    except S3StorageError as exc:
        logger.warning(
            "delete_generated_s3_error doc_id=%s key=%s error=%s", doc_id, s3_key, exc)
    # Delete MongoDB record
    try:
        mongo_adapter.delete_generated_document(doc_id)
    except MongoStorageError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return DeleteGeneratedResponse(
        id=doc_id,
        deleted_s3_key=s3_key,
        message="Generated document deleted successfully.",
    )


def _generate_document_from_payload(
    payload: DocumentGenerationRequest,
) -> DocumentGenerationResponse:
    """Resolve payload, generate or retrieve from cache, and return response."""
    logger.info(
        "generate_request_received content_id=%s version=%s",
        payload.content_id,
        payload.version,
    )
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

    if s3_adapter.object_exists(output_key) and not payload.force_regenerate:
        return _handle_cache_hit_response(
            payload=payload,
            resolved_payload=resolved_payload,
            mongo_adapter=mongo_adapter,
            s3_adapter=s3_adapter,
            file_name=file_name,
            output_ext=output_ext,
            output_key=output_key,
        )

    file_bytes = _generate_output_bytes(
        output_ext, resolved_payload, file_name)
    return _store_and_build_response(
        payload=payload,
        resolved_payload=resolved_payload,
        mongo_adapter=mongo_adapter,
        s3_adapter=s3_adapter,
        file_name=file_name,
        output_ext=output_ext,
        output_key=output_key,
        file_bytes=file_bytes,
    )


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


def close_cached_adapters() -> None:
    """Close singleton adapters during app shutdown for clean resource handling."""
    s3_adapter = _get_s3_adapter()
    mongo_adapter = _get_mongo_adapter()
    s3_adapter.close()
    mongo_adapter.close()
    _get_s3_adapter.cache_clear()
    _get_mongo_adapter.cache_clear()


def _normalize_extension(extension: str) -> str:
    if extension == "dox":
        return "docx"
    if extension == "htm":
        return "html"
    if extension == "ppt":
        return "pptx"
    return extension


def _doc_to_record(doc: dict) -> GeneratedDocumentRecord:
    return GeneratedDocumentRecord(
        id=str(doc["_id"]),
        content_id=doc.get("content_id", ""),
        version=doc.get("version", 0),
        file_name=doc.get("file_name", ""),
        extension=doc.get("extension", ""),
        output_file_s3_key=doc.get("output_file_s3_key", ""),
        source_content_updated_at=_normalize_datetime_for_response(
            doc.get("source_content_updated_at")
        ),
        created_at=_normalize_datetime_for_response(doc.get("created_at")),
        updated_at=_normalize_datetime_for_response(doc.get("updated_at")),
    )


def _get_generated_doc_or_404(doc_id: str, mongo_adapter: MongoDbStorageAdapter) -> dict:
    try:
        doc = mongo_adapter.get_generated_document_by_id(doc_id)
    except MongoStorageError as exc:
        detail = str(exc)
        if "Invalid" in detail:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=detail) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY, detail=detail) from exc
    if doc is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No generated document found with id={doc_id}",
        )
    return doc


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
    stored_content = _load_stored_content(payload, mongo_adapter)
    upload_metadata = _load_upload_metadata(stored_content, mongo_adapter)

    merged = payload.model_dump(mode="python")
    merged["extracted_data"] = _load_extracted_data(stored_content)
    merged["source_content_updated_at"] = _normalize_datetime_for_response(
        stored_content.get("updated_at")
    )

    extract_media = True
    store_media = True
    if upload_metadata is not None:
        merged["original_filename"] = upload_metadata.get("original_filename")
        merged["stored_filename"] = upload_metadata.get("stored_filename")
        merged["extension"] = upload_metadata.get("extension")
        extract_media = upload_metadata.get("extract_media", True)
        store_media = upload_metadata.get("store_media", True)

    extracted_data = merged.get("extracted_data")
    if isinstance(extracted_data, dict) and extract_media:
        try:
            _hydrate_media_from_s3(
                extracted_data, _get_s3_adapter(), store_media)
        except S3StorageError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(exc),
            ) from exc

    return ResolvedDocumentGenerationPayload.model_validate(merged)


def _handle_cache_hit_response(
    *,
    payload: DocumentGenerationRequest,
    resolved_payload: ResolvedDocumentGenerationPayload,
    mongo_adapter: MongoDbStorageAdapter,
    s3_adapter: S3StorageAdapter,
    file_name: str,
    output_ext: str,
    output_key: str,
) -> DocumentGenerationResponse:
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
        presigned_url = s3_adapter.generate_presigned_download_url(
            output_key,
            expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
        )
    except (MongoStorageError, S3StorageError) as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(exc),
        ) from exc

    return _build_generation_response(
        generated_document_id=generated_document_id,
        file_name=file_name,
        output_key=output_key,
        presigned_url=presigned_url,
        output_ext=output_ext,
    )


def _generate_output_bytes(
    output_ext: str,
    resolved_payload: ResolvedDocumentGenerationPayload,
    file_name: str,
) -> bytes:
    if output_ext == "pdf":
        return _run_pdf(resolved_payload, file_name)
    if output_ext == "pptx":
        return _run_pptx(resolved_payload, file_name)
    if output_ext == "html":
        return _run_html(resolved_payload, file_name)
    if output_ext == "md":
        return _run_md(resolved_payload, file_name)
    if output_ext == "txt":
        return _run_txt(resolved_payload, file_name)
    return _run_docx(resolved_payload, file_name)


def _store_and_build_response(
    *,
    payload: DocumentGenerationRequest,
    resolved_payload: ResolvedDocumentGenerationPayload,
    mongo_adapter: MongoDbStorageAdapter,
    s3_adapter: S3StorageAdapter,
    file_name: str,
    output_ext: str,
    output_key: str,
    file_bytes: bytes,
) -> DocumentGenerationResponse:
    generated_uploaded = False
    try:
        s3_adapter.upload_bytes(
            file_bytes,
            key=output_key,
            content_type=_content_type_for_extension(output_ext),
        )
        generated_uploaded = True
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
            output_key,
            expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
        )
    except (S3StorageError, MongoStorageError) as exc:
        if generated_uploaded and isinstance(exc, MongoStorageError):
            try:
                s3_adapter.delete_key(output_key)
            except S3StorageError:
                logger.warning(
                    "generate_output_rollback_failed content_id=%s version=%s output_key=%s",
                    payload.content_id,
                    payload.version,
                    output_key,
                )
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

    logger.info(
        "generate_request_completed content_id=%s version=%s generated_id=%s",
        payload.content_id,
        payload.version,
        generated_document_id,
    )
    return _build_generation_response(
        generated_document_id=generated_document_id,
        file_name=file_name,
        output_key=output_key,
        presigned_url=presigned_url,
        output_ext=output_ext,
    )


def _build_generation_response(
    *,
    generated_document_id: str,
    file_name: str,
    output_key: str,
    presigned_url: str,
    output_ext: str,
) -> DocumentGenerationResponse:
    expires_at = datetime.now(timezone.utc) + \
        timedelta(seconds=_PRESIGNED_URL_EXPIRY_SECONDS)
    return DocumentGenerationResponse(
        id=generated_document_id,
        file_name=file_name,
        output_file_s3_key=output_key,
        download_url=presigned_url,
        url_expires_in_seconds=_PRESIGNED_URL_EXPIRY_SECONDS,
        url_expires_at=expires_at.isoformat(),
        extension=output_ext,
    )


def _load_stored_content(
    payload: DocumentGenerationRequest,
    mongo_adapter: MongoDbStorageAdapter,
) -> dict:
    try:
        stored_content = mongo_adapter.get_content(
            content_id=payload.content_id,
            version=payload.version,
        )
    except MongoStorageError as exc:
        detail = str(exc)
        if "Invalid content_id" in detail:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=detail,
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=detail,
        ) from exc

    if stored_content is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=(
                f"No extracted content found for content_id={payload.content_id} "
                f"and version={payload.version}"
            ),
        )
    return stored_content


def _load_upload_metadata(
    stored_content: dict,
    mongo_adapter: MongoDbStorageAdapter,
) -> dict | None:
    upload_id = stored_content.get("upload_id")
    if not (isinstance(upload_id, str) and upload_id):
        return None
    try:
        return mongo_adapter.get_upload(upload_id)
    except MongoStorageError:
        return None


def _load_extracted_data(stored_content: dict) -> object:
    data_s3_key = stored_content.get("data_s3_key")
    if data_s3_key:
        try:
            raw_bytes = _get_s3_adapter().download_bytes(data_s3_key)
            if len(raw_bytes) > _MAX_EXTRACTED_JSON_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=(
                        "Extracted payload is too large to render safely "
                        f"({len(raw_bytes)} bytes > {_MAX_EXTRACTED_JSON_BYTES} bytes)."
                    ),
                )
            return json.loads(raw_bytes)
        except (S3StorageError, json.JSONDecodeError) as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to load extracted payload from storage: {str(exc)}",
            ) from exc
    # Backward-compat: older records stored the payload inline.
    return stored_content.get("data")


def _normalize_datetime_for_response(value: object | None) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _hydrate_media_from_s3(  # NOSONAR
    node: object,
    s3_adapter: S3StorageAdapter,
    store_media: bool = True,
    *,
    depth: int = 0,
    state: dict[str, int] | None = None,
) -> None:
    """Hydrate media from S3, but only if media was stored externally (store_media=true).

    When store_media=false, media is already inline as base64 and doesn't need S3 lookup.
    """
    if depth > _MAX_HYDRATION_DEPTH:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Extracted payload nesting exceeds safe depth ({_MAX_HYDRATION_DEPTH}).",
        )

    if state is None:
        state = {"visited": 0}
    state["visited"] += 1
    if state["visited"] > _MAX_HYDRATION_NODES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=(
                f"Extracted payload object graph exceeds safe size"
                f" ({_MAX_HYDRATION_NODES} nodes)."
            ),
        )

    if isinstance(node, dict):
        s3_key = node.get("s3_key")
        has_inline_blob = bool(node.get("base64") or node.get("base64_data"))

        # Only download from S3 if store_media=true AND s3_key exists AND no inline blob
        if store_media and isinstance(s3_key, str) and s3_key and not has_inline_blob:
            blob = s3_adapter.download_bytes(s3_key)
            if len(blob) > _MAX_MEDIA_BYTES:
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=(
                        "Media item is too large to hydrate safely "
                        f"({len(blob)} bytes > {_MAX_MEDIA_BYTES} bytes)."
                    ),
                )
            encoded = base64.b64encode(blob).decode("ascii")
            node["base64"] = encoded
            node["base64_data"] = encoded

        for value in node.values():
            _hydrate_media_from_s3(
                value,
                s3_adapter,
                store_media,
                depth=depth + 1,
                state=state,
            )
        return

    if isinstance(node, list):
        for item in node:
            _hydrate_media_from_s3(
                item,
                s3_adapter,
                store_media,
                depth=depth + 1,
                state=state,
            )


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
