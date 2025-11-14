import pandas as pd
import json
from datetime import datetime
import logging
from src.core.api_client import PrecificaAPIClient

def process_api_results(found_products: list) -> pd.DataFrame:
    rows = []
    coleta_ts = datetime.now()
    #target = target_skus or set()
    for item in found_products or []:
        sku = item.get("sku")
        #if not sku or (target and sku not in target):
            #continue
        reference_code = item.get("reference_code", "")
        domains = (item.get("last_scan") or {}).get("data", []) or []
        for d in domains:
            rows.append({
                "data_coleta_preco": coleta_ts,
                "SKU": sku,
                "REFERENCE_CODE": reference_code,
                "DOMAIN": (d.get("domain") or "").strip(),
                "DATE_OCCURRENCE": d.get("date_occurrence"),
                "AVAILABILITY": d.get("availability"),
                "PRICE": d.get("price"),
                "OFFER_PRICE": d.get("offer_price"),
                "SOLD_BY": d.get("sold_by"),
                "SELLERS": json.dumps(d.get("sellers") or [], ensure_ascii=False),
                "pack_price": d.get("pack_price"),
                "pack_info": d.get("pack_info"),
                "fator": d.get("fator"),
                "factor_price": d.get("factor_price"),
                "factor_offer_price": d.get("factor_offer_price"),
                "from_price": d.get("from_price"),
            })
    
    return pd.DataFrame(rows) if rows else pd.DataFrame()

