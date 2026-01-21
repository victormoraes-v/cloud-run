from __future__ import annotations

from typing import Optional

from google.cloud import storage


def get_client() -> storage.Client:
    return storage.Client()


def download_text(bucket: str, blob_path: str) -> Optional[str]:
    client = get_client()
    b = client.bucket(bucket)
    blob = b.blob(blob_path)
    if not blob.exists():
        return None
    return blob.download_as_text(encoding="utf-8")


def upload_bytes(bucket: str, blob_path: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    client = get_client()
    b = client.bucket(bucket)
    blob = b.blob(blob_path)
    blob.upload_from_string(data, content_type=content_type)


def upload_file(bucket: str, blob_path: str, local_path: str, content_type: str = "application/octet-stream") -> None:
    client = get_client()
    b = client.bucket(bucket)
    blob = b.blob(blob_path)
    blob.upload_from_filename(local_path, content_type=content_type)
