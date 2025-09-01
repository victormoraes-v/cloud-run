import time
import json
import functions_framework

# Importa os módulos refatorados da sua aplicação
import config
from services import gcp_services
from services import source_db
import dataform_generator
from services.github_services import GitHubAPI

@functions_framework.http
def main(request):
    """
    Função principal que orquestra todo o fluxo de geração de modelos do Dataform.
    
    O fluxo de trabalho é o seguinte:
    1.  Carrega e valida a configuração a partir de variáveis de ambiente.
    2.  Obtém a lista de tabelas pendentes de uma tabela de configuração no BigQuery.
    3.  Se não houver tabelas, encerra com sucesso.
    4.  Obtém as credenciais necessárias (banco de dados de origem, token do GitHub) do Secret Manager.
    5.  Inicializa os clientes para interagir com o banco de dados de origem e a API do GitHub.
    6.  Cria uma nova branch no repositório do GitHub para versionar as mudanças.
    7.  Itera sobre cada tabela pendente:
        a. Obtém o schema da tabela no banco de dados de origem.
        b. Gera uma cláusula SELECT com SAFE_CAST para garantir a tipagem correta no BigQuery.
        c. Gera o conteúdo completo do arquivo .sqlx para o modelo 'raw' no Dataform.
        d. Cria o novo arquivo .sqlx na nova branch do GitHub.
        e. Gera o bloco de código DDL ('CREATE OR REPLACE EXTERNAL TABLE').
    8.  Após o loop, agrupa todos os blocos DDL e os insere/atualiza em um arquivo de operações.
    9.  Atualiza o arquivo de declaração de fontes (.js) com as novas tabelas externas.
    10. Retorna uma resposta de sucesso com o link para a nova branch.
    """
    try:
        # 1. Configuração é validada na importação do módulo 'config'
        print("Configuração carregada e validada.")
        
        # 2. Obter dados de entrada a partir do payload da requisição
        request_json = request.get_json(silent=True)
        config_table_id = request_json.get('config_table_id') if request_json else None
        if not config_table_id:
            return ("'config_table_id' não encontrado no payload.", 400)

        # 3. Obter a lista de tabelas pendentes do BigQuery
        pending_tables = gcp_services.get_pending_tables(config.GCP_PROJECT_ID, config_table_id)
        if not pending_tables:
            return ("Nenhuma tabela pendente encontrada. Finalizando com sucesso.", 200)

        # 4. Obter credenciais e configurar clientes
        table_config_name = config_table_id.split(".")[-1]
        
        # Obter credenciais do banco de dados de origem
        db_secret_id = config.TABLE_TO_DB_SECRET_MAP[table_config_name]
        db_connection_json = gcp_services.get_secret(config.GCP_PROJECT_ID, db_secret_id)
        db_engine = source_db.get_db_engine(db_connection_json)
        
        # Obter token do GitHub e inicializar cliente da API
        github_token = gcp_services.get_secret(config.GCP_PROJECT_ID, config.GITHUB_TOKEN_SECRET_ID)
        github_client = GitHubAPI(token=github_token, user=config.GITHUB_USER, repo=config.GITHUB_REPO)

        # 5. Preparar a branch no GitHub
        timestamp = int(time.time())
        new_branch_name = f"feat/add-tables-batch-{timestamp}"
        github_client.create_branch(config.BASE_BRANCH, new_branch_name)

        # 6. Processar cada tabela pendente
        instance_name = config.TABLE_TO_INSTANCE_MAP[table_config_name]
        db_details = json.loads(db_connection_json)['connections'][0]
        database_name = db_details['database_name']

        all_ddl_blocks = [] # Lista para armazenar os blocos DDL
        tables_added_to_sources = [] # Lista para rastrear quais tabelas são realmente novas

        for table_data in pending_tables:
            source_table = table_data['source_table_name']
            target_table = table_data['target_table_name']

            incremental_join_col = table_data.get('incremental_join_clause')
            filter_column = table_data['filter_column'] if incremental_join_col else None
            
            print(f"--- Processando Tabela: {source_table} -> {target_table} ---")

            # 6a. Obter o schema da origem
            schema = source_db.get_source_table_schema(db_engine, source_table)
            
            # 6b. Gerar a cláusula SELECT
            select_clause = source_db.generate_safe_cast_select(
                schema=schema,
                source_table_name=f"ext_{target_table.lower()}",
                partition_col_to_add=filter_column
            )
            # 6c. Gerar o conteúdo completo do arquivo .sqlx para o modelo 'raw'
            sqlx_content = dataform_generator.generate_sqlx_content(
                instance_name=instance_name,
                target_dataset=table_data['target_dataset'],
                migration_type=table_data['migration_type'],
                partition_column=table_data['partition_column'],
                filter_column=table_data['filter_column'],
                select_clause=select_clause
            )

            # 6d. Criar o arquivo .sqlx do modelo 'raw' no repositório
            raw_file_path = f"{config.RAWS_DIR_PATH_TEMPLATE.format(instance=instance_name)}/{target_table}.sqlx"
            commit_message = f"feat: Adicionar modelo raw para {target_table}"
            github_client.upsert_sqlx_file(raw_file_path, new_branch_name, sqlx_content, commit_message)

            # 6e. Gerar o bloco de código DDL ('CREATE OR REPLACE EXTERNAL TABLE')
            gcs_uri = config.GCS_BASE_URI_TEMPLATE.format(instance=instance_name, database=database_name.lower()) + f"/{source_table.lower()}"
            ddl_block = dataform_generator.generate_ddl_operation_block(
                target_dataset=table_data['target_dataset'],
                target_table=f"ext_{target_table.lower()}", # Padrão para nomes de tabelas externas
                partition_column=table_data['partition_column'],
                gcs_uri=gcs_uri
            )
            all_ddl_blocks.append(ddl_block)

        # 7. Atualizar o arquivo de operações DDL (após o loop)
        if all_ddl_blocks:
            ddl_file_path = config.DDL_OPERATIONS_FILE_PATH_TEMPLATE.format(instance=instance_name)
            combined_ddl_content = "\n\n---\n\n".join(all_ddl_blocks)
            commit_message = f"feat: Adicionar/Atualizar DDLs de tabelas externas para o lote {timestamp}"
            
            github_client.upsert_file(
                file_path=ddl_file_path,
                branch=new_branch_name,
                new_content_block=combined_ddl_content,
                commit_message=commit_message
            )

        # 8. Atualizar o arquivo de declaração de fontes (.js)
        sources_file_path = config.SOURCES_FILE_PATH_TEMPLATE.format(instance=instance_name)
        original_sources_content, sha = github_client.get_file_content(sources_file_path, new_branch_name)
        
        updated_sources_content, tables_added_to_sources = dataform_generator.generate_source_js_update(
            original_content=original_sources_content,
            tables_to_add=pending_tables
        )
        
        if tables_added_to_sources:
            github_client.update_file(
                file_path=sources_file_path,
                branch=new_branch_name,
                new_content=updated_sources_content,
                sha=sha,
                message=f"feat: Adicionar fontes para o lote {timestamp}"
            )
        else:
            print("Nenhuma fonte nova para adicionar. O arquivo de fontes não foi modificado.")

        # 9. Atualizar as flags na tabela de controle do BigQuery
        gcp_services.update_table_creation_flags(
            project_id=config.GCP_PROJECT_ID,
            config_table_id=config_table_id,
            processed_tables=pending_tables
        )

        # 10. Retornar resposta de sucesso
        final_message = f"Push para a branch '{new_branch_name}' realizado com sucesso, contendo definições para {len(pending_tables)} tabela(s)."
        return ({"message": final_message, "branch": new_branch_name}, 200)

    except Exception as e:
        # Gerenciamento de erro genérico para capturar qualquer falha
        print(f"ERRO FATAL: Um erro inesperado ocorreu: {e}")
        # Em um cenário real, você poderia adicionar lógica para deletar a branch criada em caso de falha.
        return ("Ocorreu um erro interno. Verifique os logs da função.", 500)