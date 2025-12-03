import time
import json
import requests
import functions_framework

# Importa os módulos refatorados da sua aplicação
import config
from services import gcp_services
from services import source_db
import dataform_generator
from services.github_services import GitHubAPI
from db.factory import get_loader

@functions_framework.http
def main(request):
    """
    Função principal que orquestra todo o fluxo de geração de modelos do Dataform.
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
        
        db_secret_id = config.TABLE_TO_DB_SECRET_MAP[table_config_name]
        db_connection_json = gcp_services.get_secret(config.GCP_PROJECT_ID, db_secret_id)
        # db_engine = source_db.get_db_engine(db_connection_json)
        
        github_token = gcp_services.get_secret(config.GCP_PROJECT_ID, config.GITHUB_TOKEN_SECRET_ID)
        github_client = GitHubAPI(token=github_token, user=config.GITHUB_USER, repo=config.GITHUB_REPO)

        # 5. Preparar a branch no GitHub
        timestamp = int(time.time())
        new_branch_name = f"feat/add-tables-batch-{timestamp}"
        github_client.create_branch(config.BASE_BRANCH, new_branch_name)

        # 6. Obter o estado ATUAL dos arquivos de configuração do Dataform ANTES do loop.
        instance_name = config.TABLE_TO_INSTANCE_MAP[table_config_name]
        
        sources_file_path = config.SOURCES_FILE_PATH_TEMPLATE.format(instance=instance_name)
        ddl_file_path = config.DDL_OPERATIONS_FILE_PATH_TEMPLATE.format(instance=instance_name)
        
        try:
            original_sources_content = github_client.get_file_content(sources_file_path, new_branch_name)[0]
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Arquivo de fontes '{sources_file_path}' não encontrado. Será criado um novo.")
                original_sources_content = ""
            else: raise

        try:
            original_ddl_content = github_client.get_file_content(ddl_file_path, new_branch_name)[0]
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                print(f"Arquivo DDL '{ddl_file_path}' não encontrado. Será criado um novo.")
                original_ddl_content = 'config { type: "operations" }'
            else: raise

        # 7. Processar cada tabela pendente, construindo listas de mudanças
        new_source_blocks = []
        new_ddl_blocks = []
        db_details = json.loads(db_connection_json)['connections'][0]
        database_name = db_details['database_name']
        data_source_type = db_details['data_source_type']

        loader = get_loader(data_source_type)
        db_engine = loader.get_engine(db_connection_json)

        for table_data in pending_tables:
            source_table = table_data['source_table_name']
            target_table = table_data['target_table_name']
            
            # --- CORREÇÃO: Revertido para o nome original da sua coluna ---
            incremental_join_col = table_data.get('incremental_join_clause')
            
            filter_column_incremental_join = table_data['filter_column'] if incremental_join_col else None
            
            print(f"--- Processando Tabela: {source_table} -> {target_table} ---")

            # 7a. Lógica para o modelo RAW (.sqlx)
            # schema = source_db.get_source_table_schema(db_engine, source_table)
            schema = loader.get_schema(db_engine, source_table)
            # select_clause = source_db.generate_safe_cast_select(
            #     schema=schema,
            #     source_table_name=f"ext_{target_table.lower().replace('__', '_')}",
            #     partition_col_to_add=filter_column_incremental_join # Usa a coluna de filtro para o SELECT
            # )
            select_clause = loader.generate_select_safe_cast(
                schema=schema,
                table=f"ext_{target_table.lower().replace('__', '_')}",
                partition_col=filter_column_incremental_join
            )
            
            sqlx_content = dataform_generator.generate_sqlx_content(
                instance_name=instance_name,
                target_dataset=table_data['target_dataset'],
                target_table=target_table,
                migration_type=table_data['migration_type'],
                partition_column=table_data['partition_column'],
                filter_column=table_data['filter_column'],
                select_clause=select_clause
            )
            raw_file_path = f"{config.RAWS_DIR_PATH_TEMPLATE.format(instance=instance_name)}/{target_table}.sqlx"
            commit_message_raw = f"feat: Adicionar/Atualizar modelo bronze para {target_table}"
            github_client.upsert_sqlx_file(raw_file_path, new_branch_name, sqlx_content, commit_message_raw)

            # 7b. Lógica para o arquivo de FONTES (.js)
            external_name = f"ext_{target_table.lower().replace('__', '_')}"
            if f'name: "{external_name}"' not in original_sources_content:
                source_block = dataform_generator.generate_source_js_block(table_data)
                new_source_blocks.append(source_block)

            # 7c. Lógica para o arquivo de DDL (.sqlx)
            gcs_uri = config.GCS_BASE_URI_TEMPLATE.format(instance=instance_name, database=database_name.lower()) + f"/{source_table.lower()}"
            ddl_block = dataform_generator.generate_ddl_operation_block(
                target_dataset=table_data['target_dataset'],
                target_table=external_name,
                partition_column=table_data['partition_column'],
                gcs_uri=gcs_uri
            )
            if f"EXTERNAL TABLE `grp-venancio-prd-dados.{table_data['target_dataset']}.{external_name}`" not in original_ddl_content:
                 new_ddl_blocks.append(ddl_block)
        
        # 8. Commitar as mudanças nos arquivos de configuração, se houver
        commit_message_suffix = f"para o lote {timestamp}"

        if new_source_blocks:
            final_sources_content = original_sources_content + "\n\n" + "\n\n".join(new_source_blocks)
            github_client.create_or_update_file(
                file_path=sources_file_path,
                branch=new_branch_name,
                new_content=final_sources_content,
                commit_message=f"feat: Adicionar fontes {commit_message_suffix}"
            )
        else:
            print("Nenhuma fonte nova para adicionar. Arquivo de fontes não modificado.")

        if new_ddl_blocks:
            final_ddl_content = original_ddl_content + "\n\n---\n\n" + "\n\n---\n\n".join(new_ddl_blocks)
            github_client.create_or_update_file(
                file_path=ddl_file_path,
                branch=new_branch_name,
                new_content=final_ddl_content,
                commit_message=f"feat: Adicionar DDLs {commit_message_suffix}"
            )
        else:
            print("Nenhum DDL novo para adicionar. Arquivo de operações DDL não modificado.")
            
        # 9. Atualizar as flags no BigQuery
        gcp_services.update_table_creation_flags(
            project_id=config.GCP_PROJECT_ID,
            config_table_id=config_table_id,
            processed_tables=pending_tables
        )
        
        # 10. Retornar resposta de sucesso
        final_message = f"Push para a branch '{new_branch_name}' realizado com sucesso, contendo definições para {len(pending_tables)} tabela(s)."
        return ({"message": final_message, "branch": new_branch_name}, 200)

    except Exception as e:
        print(f"ERRO FATAL: Um erro inesperado ocorreu: {e}")
        # Lembre-se que a branch criada pode precisar ser deletada manualmente em caso de falha.
        return ("Ocorreu um erro interno. Verifique os logs da função.", 500)