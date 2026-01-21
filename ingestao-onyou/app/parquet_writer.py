from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import pyarrow as pa
import pyarrow.parquet as pq


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _chunk_records(records: List[Dict[str, Any]], max_records: int) -> Iterable[Tuple[int, List[Dict[str, Any]]]]:
    if max_records <= 0:
        yield 0, records
        return
    for i in range(0, len(records), max_records):
        yield i // max_records, records[i : i + max_records]


def write_parquet_files(
    records: List[Dict[str, Any]],
    entity: str,
    id_execucao: str,
    dt_ingestao: datetime,
    out_dir: str,
    compression: str,
    max_records_per_file: int,
) -> List[str]:
    """
    Gera 1..N arquivos parquet em out_dir e retorna a lista de paths locais.
    Mantém nested structures (dict/list) sem flatten.
    """
    os.makedirs(out_dir, exist_ok=True)
    ingestion_ts = _now_utc_iso()

    local_paths: List[str] = []

    for part_idx, chunk in _chunk_records(records, max_records_per_file):
        # adiciona auditoria SEM alterar campos originais
        enriched: List[Dict[str, Any]] = []
        for r in chunk:
            if not isinstance(r, dict):
                # fallback: guarda bruto
                r = {"_raw": r}
            rr = dict(r)
            rr["dt_ingestao"] = dt_ingestao
            rr["id_execucao"] = id_execucao
            enriched.append(rr)

        table = pa.Table.from_pylist(enriched)

        filename = f"{entity}_part={part_idx:05d}_{uuid.uuid4().hex}.parquet"
        local_path = os.path.join(out_dir, filename)

        pq.write_table(
            table,
            local_path,
            compression=compression,
            use_dictionary=True,
            write_statistics=True,
        )

        local_paths.append(local_path)

    return local_paths
