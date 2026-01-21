from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import requests


@dataclass(frozen=True)
class AuthToken:
    value: str  # "Bearer ..."


def refresh_token(
    session: requests.Session,
    base_url: str,
    subscription_key: str,
    refresh_token_old: str,
    timeout: int,
) -> AuthToken:
    # endpoint usa oldToken em query param
    url = f"{base_url.rstrip('/')}/user/profiles/auth/token/refresh"
    headers: Dict[str, str] = {"Ocp-Apim-Subscription-Key": subscription_key}

    resp = session.put(url, headers=headers, params={"oldToken": refresh_token_old}, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"Auth refresh failed: status={resp.status_code}, body={resp.text[:2000]}")

    data = resp.json()
    token = data.get("payload", {}).get("token")
    if not token:
        raise RuntimeError("Auth refresh: token missing in response payload")

    return AuthToken(value=f"Bearer {token}")
