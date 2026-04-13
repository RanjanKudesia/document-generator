# document-generator

FastAPI microservice for generating output documents from structured content.

## Features

- Single generation endpoint: `POST /generate`
- Accepts `db_record_id` from content-extractor response and loads `extracted_data` from MongoDB
- Downloads embedded media directly from S3 using stored `s3_key`
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

## Notes

- If `extension` is missing, generation defaults to docx.
- Set `CONTENT_EXTRACTOR_BASE_URL=https://content-extractor-production-6a80.up.railway.app` in `.env`.
- For PPTX generation, install `python-pptx` (already listed in requirements).
- Invalid extension values return HTTP 400 with supported values.
