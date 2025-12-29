import pandas as pd

def parse_sheet_name(value):
    """
    Converte sheet_name STRING para int se for número.
    Exemplos:
      "0" → 0
      "Info Lojas" → "Info Lojas"
      None → None (usar default do pandas)
    """
    if value is None or value == "":
        return 0

    try:
        return int(value)
    except ValueError:
        return value


def parse_usecols(value: str | None):
    """
    Interpreta usecols vindo do BigQuery:
    Formatos aceitos:
      "A:D" → Excel range
      "0,1,3" → índices
      "COD,NOME,UF" → nomes
    """
    if value is None or value == "":
        return None

    # Se for lista por índice → converte para lista
    if "," in value:
        return [v.strip() for v in value.split(",")]

    # Caso seja algo como A:D → deixa string para pandas interpretar
    return value


def read_file_from_smb(file_object, cfg: dict):
    """
    Lê arquivos SMB em pandas com parâmetros vindos da config BigQuery.
    Retorna:
      → DataFrame (se NÃO chunksize)
      → Iterator de chunks (se chunksize informado)
    """

    file_format = cfg.get("file_format", "csv")
    chunksize = cfg.get("chunksize", None)

    # Campos comuns
    header = cfg.get("header", None)
    usecols = parse_usecols(cfg.get("usecols", None))

    # ============= XLSX =============
    if file_format == "xlsx":
        sheet = parse_sheet_name(cfg.get("sheet_name"))

        params = {
            "sheet_name": sheet,
            "header": cfg.get("header_row", 0) or 0,
            "decimal": cfg.get("decimal", ".") or ".",
            "header": header,
            "engine": "openpyxl"
        }

        if usecols is not None:
            params["usecols"] = usecols

        return pd.read_excel(file_object, **params)

    # ============= CSV / TXT =============
    if file_format in ("csv", "txt"):
        params = {
            "delimiter": cfg.get("delimiter", ",") or ',',
            "decimal": cfg.get("decimal", ".") or ".",
            "encoding": cfg.get("encoding", "utf-8") or 'utf-8',
            "header": header,
            "chunksize": chunksize  # se None → DataFrame único
        }

        if usecols is not None:
            params["usecols"] = usecols

        return pd.read_csv(file_object, **params)

    # ============= PARQUET =============
    if file_format == "parquet":
        # parquet não suporta header/usecols
        return pd.read_parquet(file_object)

    raise ValueError(f"Formato não suportado: {file_format}")
