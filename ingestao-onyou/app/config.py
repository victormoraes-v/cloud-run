from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

def _env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    v = os.environ.get(name, default)
    if required and (v is None or v.strip() == ""):
        raise RuntimeError(f"Missing required env var: {name}")
    return v or ""

def _utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _now_utc_iso() -> str:
    """
    Retorna timestamp UTC no formato ISO 8601 sem microssegundos.
    Ex: 2026-01-20T14:32:10Z
    """
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

@dataclass(frozen=True)
class Settings:
    # ---- ONYOU ----
    onyou_base_url: str
    onyou_subscription_key: str
    onyou_refresh_token: str
    onyou_timeout_seconds: int

    # ---- Dimensions (igual ao seu código: datas fixas) ----
    dim_cycle_updated_since: str
    dim_structure_updated_since: str
    dim_form_updated_since: str
    dim_dept_updated_since: str

    # ---- Facts ----
    facts_updated_since_offset_days: int  # hoje - 2
    facts_period_in_days: int            # 7

    # ---- Cycles selection ----
    cycles_enddate_keep_days: int        # 45
    max_cycles_per_run: int

    # ---- Output ----
    gcs_bucket: str
    gcs_prefix: str

    # ---- Parquet ----
    parquet_compression: str
    max_records_per_file: int

    # ---- Runtime ----
    source_system: str
    dt: str
    id_execucao: str
    dt_ingestao: str

    @staticmethod
    def load(id_execucao: str) -> "Settings":
        return Settings(
            onyou_base_url=_env("ONYOU_BASE_URL", "https://api.onyou.com.br"),
            onyou_subscription_key=_env("ONYOU_SUBSCRIPTION_KEY", required=True),
            onyou_refresh_token=_env("ONYOU_REFRESH_TOKEN", required=True),
            onyou_timeout_seconds=int(_env("ONYOU_TIMEOUT_SECONDS", "60")),

            # defaults iguais ao “espírito” do seu script
            dim_cycle_updated_since=_env("DIM_CYCLE_UPDATED_SINCE", "2020-12-09T16:09:53+00:00"),
            dim_structure_updated_since=_env("DIM_STRUCTURE_UPDATED_SINCE", "2020-02-01T00:00:00.52Z"),
            dim_form_updated_since=_env("DIM_FORM_UPDATED_SINCE", "2020-02-01T00:00:00.52Z"),
            dim_dept_updated_since=_env("DIM_DEPT_UPDATED_SINCE", "2020-02-01T00:00:00.52Z"),

            facts_updated_since_offset_days=int(_env("FACTS_UPDATED_SINCE_OFFSET_DAYS", "2")),
            facts_period_in_days=int(_env("FACTS_PERIOD_IN_DAYS", "7")),

            cycles_enddate_keep_days=int(_env("CYCLES_ENDDATE_KEEP_DAYS", "45")),
            max_cycles_per_run=int(_env("MAX_CYCLES_PER_RUN", "5000")),

            gcs_bucket=_env("GCS_BUCKET", required=True),
            gcs_prefix=_env("GCS_PREFIX", "api/onyou").strip("/"),

            parquet_compression=_env("PARQUET_COMPRESSION", "snappy"),
            max_records_per_file=int(_env("MAX_RECORDS_PER_FILE", "200000")),

            source_system=_env("SOURCE_SYSTEM", "onyou"),
            dt=_utc_date(),
            dt_ingestao=_now_utc_iso(),
            id_execucao=id_execucao,
        )
