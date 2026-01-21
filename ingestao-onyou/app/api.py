from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

from .auth import AuthToken


def _headers(subscription_key: str, token: Optional[AuthToken] = None) -> Dict[str, str]:
    h = {"Ocp-Apim-Subscription-Key": subscription_key}
    if token is not None:
        h["Authorization"] = token.value
    return h


def fetch_dimension_export(
    session: requests.Session,
    base_url: str,
    subscription_key: str,
    token: AuthToken,
    dimension: str,
    updated_since: str,
    timeout: int,
) -> List[Dict[str, Any]]:
    # dimension: cycle | structure | form | dept
    url = f"{base_url.rstrip('/')}/data/dimensions/{dimension}/export"
    resp = session.get(
        url,
        headers=_headers(subscription_key, token),
        params={"updatedSince": updated_since},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Dimension export failed ({dimension}): status={resp.status_code}, body={resp.text[:2000]}")

    data = resp.json()
    payload = data.get("payload", [])
    if payload is None:
        payload = []
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected payload type for {dimension}: {type(payload)}")
    return payload


def fetch_fact_by_cycle(
    session: requests.Session,
    base_url: str,
    subscription_key: str,
    token: AuthToken,
    fact: str,
    cycle_id: str,
    updated_since: str,
    period_in_days: int,
    timeout: int,
) -> List[Dict[str, Any]]:
    # fact: evaluation/answer | evaluation/rating
    url = f"{base_url.rstrip('/')}/data/facts/{fact}/{cycle_id}/export"
    resp = session.get(
        url,
        headers=_headers(subscription_key, token),
        params={"updatedSince": updated_since, "periodInDays": period_in_days},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(
            f"Fact export failed ({fact}, cycle={cycle_id}): status={resp.status_code}, body={resp.text[:2000]}"
        )

    data = resp.json()
    payload = data.get("payload", [])
    if payload is None:
        payload = []
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected payload type for {fact}: {type(payload)}")
    return payload


def fetch_deletions(
    session: requests.Session,
    base_url: str,
    subscription_key: str,
    token: AuthToken,
    updated_since: str,
    period_in_days: int,
    timeout: int,
) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/data/facts/evaluation/deleted/export"
    resp = session.get(
        url,
        headers=_headers(subscription_key, token),
        params={"updatedSince": updated_since, "periodInDays": period_in_days},
        timeout=timeout,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Deletions export failed: status={resp.status_code}, body={resp.text[:2000]}")

    data = resp.json()
    payload = data.get("payload", [])
    if payload is None:
        payload = []
    if not isinstance(payload, list):
        raise RuntimeError(f"Unexpected payload type for deletions: {type(payload)}")
    return payload
