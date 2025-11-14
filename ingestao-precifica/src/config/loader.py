import os
import json
import logging
from configparser import ConfigParser
from typing import Dict, Any

try:
    from google.cloud import secretmanager
    _HAS_GCP = True
except Exception:
    _HAS_GCP = False

try:
    from dotenv import load_dotenv
    _HAS_DOTENV = True
except Exception:
    _HAS_DOTENV = False

def _load_dotenv_if_present() -> None:
    if _HAS_DOTENV:
        # Carrega .env se existir. Não falha se não existir.
        load_dotenv(override=False)

def _fetch_secret_payload(project_id: str, secret_name: str, version: str = "latest") -> Dict[str, Any]:
    if not _HAS_GCP:
        return {}
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    res = client.access_secret_version(name=name)
    payload = res.payload.data.decode("utf-8")
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        # Se não for JSON, assume formato KEY=VALUE\n
        data: Dict[str, Any] = {}
        for line in payload.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                data[k.strip()] = v.strip()
        return data

def _merge_config(env_values: Dict[str, str], secret_values: Dict[str, Any]) -> Dict[str, str]:
    merged = dict(env_values)
    merged.update({k: str(v) for k, v in secret_values.items() if v is not None})
    return merged

def load_config() -> ConfigParser:
    # 1) Local: tenta carregar .env para preencher os os.environ
    _load_dotenv_if_present()

    # 2) Carrega ENV direto
    env_values = {
        "API_BASE_URL": os.environ.get("API_BASE_URL", ""),
        "API_CLIENT_KEY": os.environ.get("API_CLIENT_KEY", ""),
        "API_SECRET_KEY": os.environ.get("API_SECRET_KEY", ""),
        "API_PLATAFORMA": os.environ.get("API_PLATAFORMA", ""),
        "API_DOMINIO": os.environ.get("API_DOMINIO", ""),
        "GCS_BUCKET": os.environ.get("GCS_BUCKET", ""),
        "GCS_PREFIX": os.environ.get("GCS_PREFIX", "raw/precifica/"),
    }

    # 3) Se GCP_SECRET_NAME e GCP_PROJECT definidos, tenta Secret Manager
    secret_data: Dict[str, Any] = {}
    secret_name = os.environ.get("GCP_SECRET_NAME")
    project_id = os.environ.get("GCP_PROJECT")
    if secret_name and project_id:
        try:
            secret_data = _fetch_secret_payload(project_id, secret_name)
        except Exception as e:
            logging.warning(f"Não foi possível ler o Secret '{secret_name}': {e}")

    merged = _merge_config(env_values, secret_data)

    # 4) Monta ConfigParser no formato esperado por api_client
    cfg = ConfigParser()
    cfg.add_section("API")
    cfg.set("API", "BASE_URL", merged["API_BASE_URL"])
    cfg.set("API", "CLIENT_KEY", merged["API_CLIENT_KEY"])
    cfg.set("API", "SECRET_KEY", merged["API_SECRET_KEY"])
    cfg.set("API", "PLATAFORMA", merged["API_PLATAFORMA"])
    cfg.set("API", "DOMINIO", merged["API_DOMINIO"])

    # Também retorna GCS via seção separada (útil para ler depois)
    if not cfg.has_section("GCS"):
        cfg.add_section("GCS")
    cfg.set("GCS", "BUCKET", merged["GCS_BUCKET"])
    cfg.set("GCS", "PREFIX", merged["GCS_PREFIX"])

    return cfg