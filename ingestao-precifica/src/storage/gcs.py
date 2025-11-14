import io
from datetime import datetime
from google.cloud import storage
import pytz

def save_df_to_gcs_csv(df, bucket_name: str, prefix: str = "raw/precifica/") -> str:
    if df.empty:
        return ""
    #now = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    tz = pytz.timezone('America/Sao_Paulo')
    now = datetime.now(tz)
    suffix_file_name = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    partition = now.strftime("%Y-%m-%d")
    df["dt_ingestao"] = now
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{prefix}dt={partition}/precifica_raw_prices_{suffix_file_name}.csv")
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8")
    blob.upload_from_string(buf.getvalue(), content_type="text/csv; charset=utf-8", timeout=1200)
    return blob.name