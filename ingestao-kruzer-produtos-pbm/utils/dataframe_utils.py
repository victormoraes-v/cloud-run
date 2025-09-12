import pandas as pd
from typing import Any
import pytz
from datetime import datetime

def transform_pbms(json_payload: Any) -> pd.DataFrame:
    """
    Aplica o tratamento/normalização do payload PBM e adiciona auditoria.

    Regras implementadas:
        - Normalização de estruturas aninhadas (explode/normalize).
        - Garantia de que 'price' é dict; caso contrário, preenche com None.
        - Remoção de colunas não utilizadas e renomeações pontuais.
        - Seleção das colunas finais.
        - Inclusão de coluna de auditoria em UTC.

    Args:
        json_payload: Objeto JSON retornado pela API.

    Returns:
        DataFrame com as colunas finais prontas para persistência.
    """
    tabela = pd.json_normalize(json_payload)
    pbms = pd.DataFrame(tabela.pbms.explode().to_list())
    data = pd.DataFrame(pbms.data.to_list())

    price_list = data.price.to_list()
    for idx, linha in enumerate(price_list):
        if not isinstance(linha, dict):
            price_list[idx] = {
                "PRODUTO": None,
                "EAN": None,
                "PRECO_MAXIMO": None,
                "maximumPrice": None,
                "PMC": None,
                "PRECO_VENDA": None,
                "DESCONTO_PADRAO": None,
                "GRUPO_PRECO": None,
                "DATA_ATUALIZACAO_PRECO": None,
                "DATA_REMOCAO": None,
            }
    price = pd.DataFrame(price_list)

    tabela = tabela.drop(columns=["pbms", "eans"], errors="ignore")
    pbms = (
        pbms.rename(columns={"pbm": "pbmspbm", "displayName": "pbmsdisplayName"})
        .drop(columns=["data"], errors="ignore")
    )
    data = data.drop(columns=["price", "ean"], errors="ignore")

    completa = pd.concat([tabela, pbms, data, price], axis=1, ignore_index=False)

    final = completa.copy()[
        [
            "_id",
            "sku",
            "code",
            "pbm",
            "pbmspbm",
            "displayName",
            "pbmsdisplayName",
            "ValorBase",
            "ValorMaximo",
            "Desconto",
            "PorcentagemDesconto",
            "maximumPrice",
            "PMC",
        ]
    ]

    # Inclusao campo dt_ingestao
    tz = pytz.timezone("America/Sao_Paulo")
    final["dt_ingestao"] = datetime.now(tz).replace(tzinfo=None)

    return final