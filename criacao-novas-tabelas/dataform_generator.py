# Arquivo: dataform_generator.py
"""
Módulo responsável por gerar o conteúdo dos arquivos do Dataform (.js e .sqlx).
"""
import textwrap

def generate_source_js_block(table_data: dict) -> str:
    """
    Gera um bloco de declaração de fonte 'declare({...});' para uma única tabela.

    Args:
        table_data: Dicionário contendo os metadados da tabela, com chaves em minúsculas.

    Returns:
        Uma string formatada com o bloco de declaração da fonte.
    """
    target_name = table_data['target_table_name'].lower()
    dataset = 'landing'
    external_name = f"ext_{target_name}".replace('__', '_')

    block_template = f"""
        declare({{
            database: "grp-venancio-prd-dados",
            schema: "{dataset}",
            name: "{external_name}"
        }});
    """
    return textwrap.dedent(block_template).strip()

def generate_sqlx_content(
    instance_name: str,
    target_dataset: str,
    target_table: str,
    migration_type: str,
    partition_column: str,
    filter_column: str,
    select_clause: str
) -> str:
    """
    Gera o conteúdo completo de um arquivo .sqlx para um modelo bronze.

    Args:
        instance_name: Nome da instância de origem (ex: 'ncr').
        target_dataset: O schema de destino no BigQuery.
        migration_type: 'INCREMENTAL' ou 'FULL'.
        partition_column: Nome da coluna de partição.
        filter_column: Nome da coluna para o filtro incremental.
        select_clause: A cláusula SELECT completa gerada anteriormente.

    Returns:
        O conteúdo formatado do arquivo .sqlx.
    """
    
    # Template usando formatação %, que é mais limpa para este caso
    # por não conflitar com as chaves {} do Dataform.
    config_block_template = """
        config {
            type: "%(type)s",
            database: "grp-venancio-prd-dados",
            schema: "%(schema)s",%(bigquery_config)s
            tags: ["%(instance)s"]
        }
    """
    
    js_block_template = """
        js {
            const constants = require("includes/constants");
            const start_date = dataform.projectConfig.vars.start_date || constants.startDate;
            const end_date = dataform.projectConfig.vars.end_date || constants.endDate;
        }
    """

    pre_operations_template = """
        pre_operations {
            ${when(incremental(),
            `DELETE FROM ${self()} WHERE DT >= '${start_date}' AND DT <= '${end_date}'`)
            }
        }
    """

    filter_template = f"""
        WHERE
            dt = (SELECT MAX(dt) FROM ${{ref("ext_{target_table.replace('__', '_')}")}}) AND
            CAST(dt_ingestao AS DATETIME) = (SELECT MAX(CAST(dt_ingestao AS DATETIME)) FROM ${{ref("ext_{target_table.replace('__', '_')}")}})
    """

    # Lógica para construir o arquivo parte por parte
    file_parts = []
    variables = {
        "schema": target_dataset,
        "instance": instance_name,
        "filter_col": filter_column,
    }

    if migration_type.upper() == 'INCREMENTAL':
        variables["type"] = "incremental"
        variables["bigquery_config"] = "\n            bigquery: {partitionBy: 'DT'},"
        file_parts.append(textwrap.dedent(js_block_template).strip())
        file_parts.append(textwrap.dedent(config_block_template % variables).strip())
        file_parts.append(textwrap.dedent(pre_operations_template % variables).strip())
    else: # FULL
        variables["type"] = "table"
        variables["bigquery_config"] = ""
        file_parts.append(textwrap.dedent(config_block_template % variables).strip())

    file_parts.append(select_clause)

    file_parts.append(textwrap.dedent(filter_template % variables).strip())

    # Junta todas as partes com duas quebras de linha
    return "\n\n".join(file_parts)

def generate_ddl_operation_block(
    target_dataset: str,
    target_table: str,
    partition_column: str,
    gcs_uri: str,
    file_format: str = 'parquet'
) -> str:
    """
    Gera um bloco de código 'CREATE OR REPLACE EXTERNAL TABLE' para uma tabela.

    Args:
        target_dataset: O schema de destino no BigQuery.
        target_table: O nome da tabela de destino.
        partition_column: A coluna de partição da tabela externa.
        gcs_uri: O URI base no Google Cloud Storage para os arquivos Parquet.
        file_format: O formato do arquivo (parquet, csv, etc).

    Returns:
        Uma string formatada com o comando DDL.
    """
    # Opções específicas para CSV
    csv_options = ""
    if file_format.lower() == 'csv':
        csv_options = """,
            field_delimiter = ',',
            allow_quoted_newlines = TRUE,
            encoding = 'UTF-8'"""
    
    ddl_template = """
        -- Define a tabela externa para %(target_table)s
        CREATE OR REPLACE EXTERNAL TABLE `%(project)s.landing.%(target_table)s`
        WITH PARTITION COLUMNS (
           dt DATE
        )
        OPTIONS (
            format = '%(file_format)s',
            uris = ['%(uri)s/*'],
            hive_partition_uri_prefix = '%(uri)s/'%(csv_options)s
        );
    """
    
    variables = {
        "project": "grp-venancio-prd-dados",
        "dataset": target_dataset,
        "target_table": target_table,
        "partition_col": partition_column,
        "uri": gcs_uri,
        "file_format": file_format,
        "csv_options": csv_options,
    }
    
    return textwrap.dedent(ddl_template % variables).strip()