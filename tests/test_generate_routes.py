from fastapi.testclient import TestClient

from app.main import app
import app.api.routes as routes


class FakeMongo:
    def get_content(self, content_id: str, version: int):
        _ = (content_id, version)
        return {
            "_id": "69f64331423c9bfe1bf883a1",
            "upload_id": "69f64331423c9bfe1bf883a2",
            "data": {"paragraphs": [], "tables": [], "media": []},
        }

    def get_upload(self, upload_id: str):
        _ = upload_id
        return {
            "original_filename": "report.docx",
            "stored_filename": "report.docx",
            "extension": "docx",
            "extract_media": True,
            "store_media": False,
        }

    def upsert_generated_document(self, **kwargs):
        _ = kwargs
        return "generated-1"

    def close(self):
        return None


class FakeS3:
    def object_exists(self, key: str) -> bool:
        _ = key
        return False

    def upload_bytes(self, data: bytes, key: str, content_type: str | None = None):
        _ = (data, key, content_type)
        return key

    def generate_presigned_download_url(self, key: str, expires_in_seconds: int = 3600) -> str:
        _ = (key, expires_in_seconds)
        return "https://example.com/download"

    def build_key(self, *parts: str) -> str:
        return "/".join(parts)

    def download_bytes(self, key: str) -> bytes:
        _ = key
        return b'{"paragraphs": [], "tables": [], "media": []}'

    def close(self):
        return None


client = TestClient(app)


def test_generate_post_uses_request_extension(monkeypatch):
    fake_mongo = FakeMongo()
    fake_s3 = FakeS3()

    monkeypatch.setattr(routes, "_get_mongo_adapter", lambda: fake_mongo)
    monkeypatch.setattr(routes, "_get_s3_adapter", lambda: fake_s3)
    monkeypatch.setattr(routes, "_generate_output_bytes",
                        lambda output_ext, resolved_payload, file_name: b"bytes")

    response = client.post(
        "/generate",
        json={
            "content_id": "69f64331423c9bfe1bf883a1",
            "version": 0,
            "extension": "pdf",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["extension"] == "pdf"
    assert payload["file_name"].endswith(".pdf")


def test_generate_get_kept_as_deprecated_endpoint():
    schema = app.openapi()
    assert "post" in schema["paths"]["/generate"]
    assert "get" in schema["paths"]["/generate"]
    assert schema["paths"]["/generate"]["get"]["deprecated"] is True


def test_generate_rejects_oversized_payload(monkeypatch):
    fake_mongo = FakeMongo()
    fake_s3 = FakeS3()

    def large_bytes(_key: str) -> bytes:
        return b"{" + (b"x" * 64) + b"}"

    monkeypatch.setattr(fake_s3, "download_bytes", large_bytes)
    monkeypatch.setattr(routes, "_MAX_EXTRACTED_JSON_BYTES", 10)
    monkeypatch.setattr(routes, "_get_mongo_adapter", lambda: fake_mongo)
    monkeypatch.setattr(routes, "_get_s3_adapter", lambda: fake_s3)

    response = client.post(
        "/generate",
        json={
            "content_id": "69f64331423c9bfe1bf883a1",
            "version": 0,
            "extension": "pdf",
        },
    )

    assert response.status_code == 413
