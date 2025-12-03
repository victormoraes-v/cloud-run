import json
import os
from google.cloud import secretmanager
from .models import CollectionConfig


def load_mongo_secret(secret_name: str) -> dict:
    client = secretmanager.SecretManagerServiceClient()
    project_id = os.getenv("GCP_PROJECT")
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"

    response = client.access_secret_version(request={"name": secret_path})
    payload = response.payload.data.decode("utf-8")
    return json.loads(payload)

