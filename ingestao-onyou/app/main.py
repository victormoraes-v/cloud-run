from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .config import Settings
from .logging_utils import setup_logging
from .http_client import build_session
from .auth import refresh_token
from .api import (
    fetch_dimension_export,
    fetch_fact_by_cycle,
    fetch_deletions,
)
from .parquet_writer import write_parquet_files
from .gcs import upload_file


def _gcs_entity_prefix(s: Settings, entity: str) -> str:
    return f"{s.gcs_prefix}/{entity}/dt={s.dt}/{s.dt_ingestao}/{s.id_execucao}"

def _parse_iso_dt(value: str) -> Optional[datetime]:
    """
    Parse best-effort timestamps from API.
    Aceita formatos com Z e com offset +00:00.
    """
    if not value:
        return None
    v = value.strip()
    try:
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        # datetime.fromisoformat não aceita bem frações com 'Z' mas aceita com +00:00
        return datetime.fromisoformat(v)
    except Exception:
        return None


def _facts_updated_since(settings: Settings) -> str:
    """
    Igual ao seu código:
    hoje = date(now) - 2 dias
    hoje_formatado = '%Y-%m-%dT%H:%M:%SZ'
    """
    hoje = datetime.now(timezone.utc).date() - timedelta(days=settings.facts_updated_since_offset_days)
    # mantém H:M:S no formato Z (00:00:00Z na prática)
    dt = datetime(hoje.year, hoje.month, hoje.day, 0, 0, 0, tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _cycles_ids_to_process(cycles: List[Dict[str, Any]], settings: Settings) -> List[str]:
    """
    data_min = hoje - 45 dias (onde hoje já é date(now)-2)
    filtra por cycleEndDate >= data_min
    """
    hoje = datetime.now(timezone.utc).date() - timedelta(days=settings.facts_updated_since_offset_days)
    data_min = hoje - timedelta(days=settings.cycles_enddate_keep_days)

    ids: List[str] = []
    for c in cycles:
        cid = c.get("id")
        end_raw = c.get("cycleEndDate")
        end_dt = _parse_iso_dt(str(end_raw)) if end_raw is not None else None
        if cid and end_dt is not None and end_dt.date() >= data_min:
            ids.append(str(cid))

    # unique mantendo ordem
    seen = set()
    unique_ids = []
    for x in ids:
        if x not in seen:
            seen.add(x)
            unique_ids.append(x)

    if len(unique_ids) > settings.max_cycles_per_run:
        unique_ids = unique_ids[: settings.max_cycles_per_run]

    return unique_ids


def _upload_records_as_parquet(
    settings: Settings,
    logger,
    entity: str,
    records: List[Dict[str, Any]],
) -> None:
    tmp_dir = f"/tmp/onyou/{entity}"
    local_files = write_parquet_files(
        records=records,
        entity=entity,
        id_execucao=settings.id_execucao,
        dt_ingestao=settings.dt_ingestao,
        out_dir=tmp_dir,
        compression=settings.parquet_compression,
        max_records_per_file=settings.max_records_per_file,
    )

    gcs_prefix = _gcs_entity_prefix(settings, entity)
    for lf in local_files:
        blob_path = f"{gcs_prefix}/{os.path.basename(lf)}"
        upload_file(settings.gcs_bucket, blob_path, lf, content_type="application/octet-stream")
        logger.info(
            "Uploaded parquet",
            extra={
                "entity": entity,
                "gcs_path": f"gs://{settings.gcs_bucket}/{blob_path}",
                "id_execucao": settings.id_execucao,
            },
        )


def run() -> None:
    id_execucao = uuid.uuid4().hex
    settings = Settings.load(id_execucao=id_execucao)
    logger = setup_logging(os.environ.get("LOG_LEVEL", "INFO"))

    logger.info("Starting onyou ingest job", extra={"id_execucao": settings.id_execucao})

    session = build_session()

    # ----- auth -----
    logger.info("Refreshing auth token", extra={"id_execucao": settings.id_execucao})
    token = refresh_token(
        session=session,
        base_url=settings.onyou_base_url,
        subscription_key=settings.onyou_subscription_key,
        refresh_token_old=settings.onyou_refresh_token,
        timeout=settings.onyou_timeout_seconds,
    )

    # ----- dimensions (updatedSince fixo, igual ao seu script) -----
    dims_cfg = [
        ("cycle", "cycle", settings.dim_cycle_updated_since),
        ("structure", "structure", settings.dim_structure_updated_since),
        ("form", "form", settings.dim_form_updated_since),
        ("dept", "dept", settings.dim_dept_updated_since),
    ]

    cycles_records: List[Dict[str, Any]] = []

    for entity, dim_name, updated_since in dims_cfg:
        logger.info("Fetching dimension", extra={"entity": entity, "id_execucao": settings.id_execucao})
        records = fetch_dimension_export(
            session=session,
            base_url=settings.onyou_base_url,
            subscription_key=settings.onyou_subscription_key,
            token=token,
            dimension=dim_name,
            updated_since=updated_since,
            timeout=settings.onyou_timeout_seconds,
        )
        logger.info("Fetched dimension", extra={"entity": entity, "records": len(records), "id_execucao": settings.id_execucao})

        if dim_name == "cycle":
            cycles_records = records

        _upload_records_as_parquet(settings, logger, entity, records)

    # ----- facts (igual ao seu script: hoje-2 + periodInDays=7) -----
    updated_since_facts = _facts_updated_since(settings)
    period = settings.facts_period_in_days

    cycle_ids = _cycles_ids_to_process(cycles_records, settings)
    logger.info("Cycles to process for facts", extra={"entity": "facts", "records": len(cycle_ids), "id_execucao": settings.id_execucao})

    # Answers
    all_answers: List[Dict[str, Any]] = []
    for cid in cycle_ids:
        recs = fetch_fact_by_cycle(
            session=session,
            base_url=settings.onyou_base_url,
            subscription_key=settings.onyou_subscription_key,
            token=token,
            fact="evaluation/answer",
            cycle_id=cid,
            updated_since=updated_since_facts,
            period_in_days=period,
            timeout=settings.onyou_timeout_seconds,
        )
        all_answers.extend(recs)

    logger.info("Fetched answers", extra={"entity": "answers", "records": len(all_answers), "id_execucao": settings.id_execucao})
    _upload_records_as_parquet(settings, logger, "answers", all_answers)

    # Ratings
    all_ratings: List[Dict[str, Any]] = []
    for cid in cycle_ids:
        recs = fetch_fact_by_cycle(
            session=session,
            base_url=settings.onyou_base_url,
            subscription_key=settings.onyou_subscription_key,
            token=token,
            fact="evaluation/rating",
            cycle_id=cid,
            updated_since=updated_since_facts,
            period_in_days=period,
            timeout=settings.onyou_timeout_seconds,
        )
        all_ratings.extend(recs)

    logger.info("Fetched ratings", extra={"entity": "ratings", "records": len(all_ratings), "id_execucao": settings.id_execucao})
    _upload_records_as_parquet(settings, logger, "ratings", all_ratings)

    # Deletions
    deletions = fetch_deletions(
        session=session,
        base_url=settings.onyou_base_url,
        subscription_key=settings.onyou_subscription_key,
        token=token,
        updated_since=updated_since_facts,
        period_in_days=period,
        timeout=settings.onyou_timeout_seconds,
    )

    logger.info("Fetched deletions", extra={"entity": "deletions", "records": len(deletions), "id_execucao": settings.id_execucao})
    _upload_records_as_parquet(settings, logger, "deletions", deletions)

    logger.info("Job completed successfully", extra={"id_execucao": settings.id_execucao})


if __name__ == "__main__":
    run()
