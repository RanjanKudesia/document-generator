"""FastAPI application entry point for the Document Generator service."""
import logging
import time

from fastapi import FastAPI
from fastapi import Request

from app.api.routes import close_cached_adapters, router as api_router
from app.config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Document Generator API",
    version="1.0.0",
    description=(
        "Generate output documents from extracted structured content.\n\n"
        "Primary workflow:\n"
        "1. Extract content via Content Extractor\n"
        "2. Call POST /generate with content_id, version, and output format\n"
        "3. Receive generated file metadata and download URL/key details"
    ),
    openapi_tags=[
        {
            "name": "health",
            "description": "Service and dependency readiness endpoints.",
        },
        {
            "name": "generation",
            "description": (
                "Generate documents from extracted content,"
                " with media handling and cache support."
            ),
        },
    ],
)
app.include_router(api_router)


@app.on_event("shutdown")
async def close_adapters() -> None:
    """Close cached adapters on application shutdown."""
    close_cached_adapters()


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log HTTP request method, path, status, and duration for every request."""
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = (time.perf_counter() - start) * 1000
    logger.info(
        "request_completed method=%s path=%s status=%s duration_ms=%.2f",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response

logger.info("Document Generator API initialized")
