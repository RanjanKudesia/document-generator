# document-generator

FastAPI microservice for generating output documents from structured content.

## Features

- Single generation endpoint: `POST /generate`
- Accepts `db_record_id` from content-extractor response and loads `extracted_data` from MongoDB
- Supports three media extraction modes from content-extractor:
  - **Extraction disabled** (`extract_media=false`): No media in payload
  - **Inline media** (`extract_media=true, store_media=false`): Media stored as base64 inline in JSON
  - **S3-backed media** (`extract_media=true, store_media=true`): Media stored on S3, reference via `s3_key` (default)
- Uploads generated document to S3 (no local output persistence)
- Returns S3 presigned download URL with 1 hour expiry
- Supported output extensions:
  - docx, dox
  - pdf
  - ppt, pptx
  - html, htm
  - md
  - txt

## Run

```bash
cd document-generator
pip install -r requirements.txt
uvicorn main:app --host 127.0.0.1 --port 8005
```

## Endpoints

- `GET /health`
- `POST /generate`

## Request Model (high level)

`POST /generate` accepts a JSON payload with either:

- `db_record_id` (preferred): looks up extraction record in MongoDB, or
- `blocks` (manual structured blocks), or
- `extracted_data` (content produced by extractor)

and optional metadata such as `extension`, `document_name`, `original_filename`, `stored_filename`.

## Example

```bash
curl -X POST http://127.0.0.1:8005/generate \
  -H "Content-Type: application/json" \
  -d '{
    "db_record_id": "<mongo_id_from_content_extractor>",
    "extension": "pdf"
  }'
```

Response includes:

- `id`
- `file_name`
- `output_file_s3_key`
- `download_url`
- `url_expires_in_seconds` (always 3600)
- `url_expires_at`
- `extension`

## Media Extraction Modes

The generator automatically adapts to the media extraction mode used by the extractor. These modes are stored in MongoDB and control how media is retrieved during generation:

### 1. No Media Extraction (`extract_media=false`)

- Extractor skips media extraction entirely
- Payload contains **no media array**
- Generator skips all S3 hydration (nothing to hydrate)
- Best for: Text-only generation or performance-sensitive scenarios

### 2. Inline Media (`extract_media=true, store_media=false`)

- Extractor extracts media but keeps it as **base64 inline** in the JSON payload
- Payload contains media array with `base64` or `base64_data` fields populated
- Generator skips S3 lookup (media already present)
- Best for: Small documents or when S3 access is limited
- Payload size: Larger (base64 encoded media inline)

### 3. S3-Backed Media (Default: `extract_media=true, store_media=true`)

- Extractor extracts media and uploads to S3
- Payload contains media array with `s3_key` references
- Generator downloads media from S3 on demand and converts to base64
- Best for: Large documents with many media assets
- Payload size: Smaller (only references)

## Notes

- If `extension` is missing, generation defaults to docx.
- Set `CONTENT_EXTRACTOR_BASE_URL=https://content-extractor-production-6a80.up.railway.app` in `.env`.
- For PPTX generation, install `python-pptx` (already listed in requirements).
- Invalid extension values return HTTP 400 with supported values.
