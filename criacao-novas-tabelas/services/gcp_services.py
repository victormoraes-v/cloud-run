# Arquivo: gcp_services.py
"""
Módulo para interagir com os serviços do Google Cloud Platform.
- Google Secret Manager
- Google BigQuery
"""
from google.cloud import bigquery, secretmanager

def get_secret(project_id: str, secret_id: str) -> str:
    """
    Busca o valor de um segredo no Google Secret Manager.

    Args:
        project_id: O ID do projeto GCP.
        secret_id: O ID do segredo.

    Returns:
        O valor do segredo como uma string decodificada.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def get_pending_tables(project_id: str, config_table_id: str) -> list[dict]:
    """
    Consulta o BigQuery para obter a lista de tabelas pendentes de criação.

    Args:
        project_id: O ID do projeto GCP.
        config_table_id: O ID completo da tabela de configuração no BigQuery.

    Returns:
        Uma lista de dicionários, onde cada dicionário representa uma tabela pendente.
    """
    client = bigquery.Client(project=project_id)
    query = f"SELECT * FROM `{config_table_id}` WHERE FLAG_TABLE_CREATED = 0"

    # Converte o resultado iterável em uma lista de dicionários,
    # com todas as chaves em minúsculas para padronização. 
    print(f"Consultando tabelas pendentes em: {config_table_id}")
    results = client.query(query).result()

    pending_tables = [
        {key.lower(): value for key, value in row.items()} 
        for row in results
    ]
    
    # Converte o resultado iterável em uma lista de dicionários para fácil manipulação
    #pending_tables = [dict(row.items()) for row in results]
    print(f"Encontradas {len(pending_tables)} tabelas para criar.")
    return pending_tables

def get_migration_table_config(project_id: str, table_config, target_table):
    client = bigquery.Client(project=project_id)
    
    query = f"""
    SELECT *
    FROM data_migration_config.{table_config}
    WHERE TARGET_TABLE_NAME = @target_table --AND active = TRUE
    """
    result = list(client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("target_table", "STRING", target_table)]
    )))


    if not result:
        raise ValueError(
            f"📌 Tabela '{target_table}' não está configurada na tabela de configuração '{table_config}'.\n"
            "🛑 Nenhum processamento foi executado.\n"
            "💡 Para habilitar este arquivo, insira uma linha na tabela com todas as configurações necessárias.\n"
            "👉 Exemplo:\n"
            f"INSERT INTO data_migration_config.{table_config} (...campos...) VALUES (...);\n"
        )

    # Converte Row para dict e coloca chaves em minúsculo
    row_dict = {k.lower(): v for k, v in dict(result[0]).items()}

    return row_dict

def get_all_migration_table_configs(project_id: str, table_config, file_name):
    """
    Busca TODAS as configurações ativas para um arquivo.
    Útil quando o mesmo arquivo aparece múltiplas vezes na tabela de configuração
    por diferentes critérios (ex: abas diferentes, target_table_name diferentes, etc.).
    
    Args:
        project_id: O ID do projeto GCP.
        table_config: Nome da tabela de configuração
        file_name: Nome do arquivo a ser processado
    
    Returns:
        Lista de dicionários, cada um representando uma configuração do arquivo
    """
    client = bigquery.Client(project=project_id)
    
    query = f"""
    SELECT *
    FROM data_migration_config.{table_config}
    WHERE source_file_name = @file_name --AND active = TRUE
    """
    result = list(client.query(query, job_config=bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("file_name", "STRING", file_name)]
    )))

    if not result:
        raise ValueError(
            f"📌 Arquivo '{file_name}' não está configurado na tabela '{table_config}'.\n"
            "🛑 Nenhum processamento foi executado.\n"
            "💡 Para habilitar este arquivo, insira uma linha na tabela com todas as configurações necessárias.\n"
            "👉 Exemplo:\n"
            f"INSERT INTO data_migration_config.{table_config} (...campos...) VALUES (...);\n"
        )

    # Converte cada Row para dict e coloca chaves em minúsculo
    configs = []
    for row in result:
        row_dict = {k.lower(): v for k, v in dict(row).items()}
        configs.append(row_dict)

    return configs

def update_table_creation_flags(project_id: str, config_table_id: str, processed_tables: list[dict]):
    """
    Atualiza o campo FLAG_TABLE_CREATED para 1 para as tabelas que foram processadas.

    Args:
        project_id: O ID do projeto GCP.
        config_table_id: O ID completo da tabela de configuração no BigQuery.
        processed_tables: A lista de dicionários das tabelas que foram processadas com sucesso.
    """
    if not processed_tables:
        print("Nenhuma tabela foi processada. Nenhuma flag para atualizar.")
        return

    # Extrai os nomes das tabelas de origem para a cláusula WHERE
    target_table_names = [table['target_table_name'] for table in processed_tables]
    
    # Formata os nomes das tabelas para uso em uma cláusula IN do SQL
    # Ex: "'tabela1', 'tabela2', 'tabela3'"
    formatted_table_list = ", ".join(f"'{name}'" for name in target_table_names)
    
    # Constrói a query de UPDATE
    update_query = f"""
        UPDATE `{config_table_id}`
        SET FLAG_TABLE_CREATED = 1
        WHERE TARGET_TABLE_NAME IN ({formatted_table_list})
    """
    
    print(f"Atualizando FLAG_TABLE_CREATED para 1 para as tabelas: {', '.join(target_table_names)}")
    
    client = bigquery.Client(project=project_id)
    
    try:
        # Executa a query de UPDATE e aguarda a conclusão
        query_job = client.query(update_query)
        query_job.result() # O .result() aguarda o job finalizar
        print(f"Flags atualizadas com sucesso para {len(target_table_names)} tabela(s).")
    except Exception as e:
        print(f"ERRO ao atualizar as flags na tabela de configuração: {e}")
        # Em um cenário de produção, você poderia levantar a exceção para
        # que a função falhe e você seja notificado.
        # raise