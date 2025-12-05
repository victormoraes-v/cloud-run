import logging
from . import (
    arvore_mkt, 
    info_lojas, 
    dark_stores, 
    info_lojas_servicos_farmaceuticos,
    produtos_pricepoint,
    canal_vendas,
    redes_infoprice,
    bairros_infoprice,
    iqvia,
    agenda_sugestao_compras,
    expurgo_pedidos_compras,
    expurgo_mapas,
    crm_class,
    crm_class_historico_cliente,
    crm_class_propz,
    produtos_margem_minima
)


# Dicionário mapeando a chave ao módulo de processamento
PROCESSOR_MAP = {
    'Árvore MKT - Servicos.xlsx': {
        "module": arvore_mkt,
        "format": "csv",
        "write_mode": "overwrite"
    },
    'Info Lojas.xlsx': {
        "module": info_lojas,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Darks_Store_Lojas.xlsx": {
        "module": dark_stores,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Info Lojas Servicos Farmaceuticos.xlsx": {
        "module": info_lojas_servicos_farmaceuticos,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "PRICEPOINT_CADASTRO PRODUTO_v2.xlsx": {
        "module": produtos_pricepoint,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Canal de Vendas.xlsx": {
        "module": canal_vendas,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Agenda_Sugestao_Compras.xlsx": {
        "module": agenda_sugestao_compras,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Expurgo_Pedidos_Compras.xlsx": {
        "module": expurgo_pedidos_compras,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Redes_InfoPrice.xlsx": {
        "module": redes_infoprice,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Bairros_InfoPrice.xlsx": {
        "module": bairros_infoprice,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Expurgo_Mapas.xlsx": {
        "module": expurgo_mapas,
        "format": "csv",
        "write_mode": "overwrite"
    },
    "Classificacao CRM.xlsx": {
        "module": crm_class,
        "format": "xlsx",
        "write_mode": "overwrite"
    },
    "Classificacao CRM - Class Historico Cliente.xlsx": {
        "module": crm_class_historico_cliente,
        "format": "xlsx",
        "write_mode": "overwrite"
    },
    "Classificacao CRM - Propz.xlsx": {
        "module": crm_class_propz,
        "format": "xlsx",
        "write_mode": "overwrite"
    },
    "PRODUTOS_MARGEM_MINIMA.xlsx": {
        "module": produtos_margem_minima,
        "format": "xlsx",
        "write_mode": "overwrite"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_DIM_GEOGRAFIA_RPE.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_DIM_CANAL.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_DIM_PDV.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_DIM_PERIODO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_DIM_PROVEDOR.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_DIM_PRODUTO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_FAT_CONTAGEM_PDV.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "RPE_117_FF_PCP_VAREJO_M_VENANCIO_FAT_DEMANDA.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_DIM_CANAL_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_DIM_GEOGRAFIA_RPE_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_DIM_PDV_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_DIM_PERIODO_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_DIM_PRODUTO_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_DIM_PROVEDOR_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_FAT_CONTAGEM_PDV_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    },
    "FF_107_FAT_DEMANDA_VENANCIO.txt": {
        "module": iqvia,
        "format": "parquet",
        "write_mode": "partitioned"
    }
}

def get_processor(filename):
    """Encontra e retorna a função de processamento correta."""
    # processor_module = PROCESSOR_MAP.get(filename)
    config = PROCESSOR_MAP.get(filename)
    if config:
        logging.info(f"Processador encontrado para '{filename}' → módulo={config['module'].__name__}, formato={config['format']}, modo={config['write_mode']}")
        return config["module"].process, config["format"], config["write_mode"]
    # if processor_module:
    #     logging.info(f"Processador específico '{processor_module.__name__}' encontrado para o arquivo '{filename}'.")
    #     return processor_module.process
    
    # Se o loop terminar sem encontrar, use o padrão
    logging.warning(
        f"Nenhum processador específico encontrado para '{filename}'.",
        extra={"json_fields": {"filename": filename}}
    )
