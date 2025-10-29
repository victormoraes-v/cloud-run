# Arquivo: config.py
"""
Centraliza a configuração da aplicação, carregando-a a partir de variáveis de ambiente.
Valida a presença de todas as configurações necessárias na inicialização.
"""
import os

# --- Configuração do Projeto GCP ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# --- Configuração do GitHub ---
GITHUB_USER = os.environ.get("GITHUB_USER")
GITHUB_REPO = os.environ.get("GITHUB_REPO")
GITHUB_TOKEN_SECRET_ID = os.environ.get("GITHUB_TOKEN_ID")
BASE_BRANCH = "main"

# --- Mapeamentos Específicos do Dataform ---
# Dicionário que mapeia a tabela de configuração para a instância de origem
TABLE_TO_INSTANCE_MAP = {
    "ncr_tables_migration_config": "ncr",
    "procfit_pbs_venancio_tables_migration_config": "procfit",
    "programare_dbfar_tables_migration_config": "programare",
    "programare_integracao_kruzer_tables_migration_config": "programare",
    "programare_integracao_vtex_tables_migration_config": "programare",
    "serv_prd_stage_kruzer_tables_migration_config": "serv_prd",
    "rm_corporerm_tables_migration_config": "rm",
    "rm_procfit_tables_migration_config": "rm",
    "serv_prd_stage_area_tables_migration_config": "serv_prd",
    "serv_prd_dw_tables_migration_config": "serv_prd",
    "siga_tables_migration_config": "siga"
}

# Dicionário que mapeia a tabela de configuração para o ID do segredo de conexão do BD
TABLE_TO_DB_SECRET_MAP = {
    "ncr_tables_migration_config": "ncr_database_connection_config",
    "procfit_pbs_venancio_tables_migration_config": "procfit_pbs_venancio_database_connection_config",
    "programare_dbfar_tables_migration_config": "programare_dbfar_database_connection_config",
    "programare_integracao_kruzer_tables_migration_config": "programare_integracao_kruzer_database_connection_config",
    "programare_integracao_vtex_tables_migration_config": "programare_integracao_vtex_database_connection_config",
    "serv_prd_stage_kruzer_tables_migration_config": "serv_prd_stage_kruzer_database_connection_config",
    "rm_corporerm_tables_migration_config": "rm_corporerm_database_connection_config",
    "rm_procfit_tables_migration_config": "rm_procfit_database_connection_config",
    "serv_prd_stage_area_tables_migration_config": "serv_prd_stage_area_database_connection_config",
    "serv_prd_dw_tables_migration_config": "serv_prd_dw_database_connection_config",
    "siga_tables_migration_config": "siga_database_connection_config"
}

# --- Caminhos de Arquivo no Repositório Dataform ---
SOURCES_FILE_PATH_TEMPLATE = "definitions/sources/{instance}.js"
RAWS_DIR_PATH_TEMPLATE = "definitions/bronze/{instance}"
DDL_OPERATIONS_FILE_PATH_TEMPLATE = "definitions/ddl/create_external_table/{instance}.sqlx"
# --- URI do Cloud Storage ---
GCS_BASE_URI_TEMPLATE = "gs://grp-venancio-prd-dados_ingestao_dataflow/{instance}/{database}/dbo"

# --- Validação de Configuração ---
def validate_config():
    """Garante que todas as variáveis de ambiente essenciais estão definidas."""
    required_vars = {
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "GITHUB_USER": GITHUB_USER,
        "GITHUB_REPO": GITHUB_REPO,
        "GITHUB_TOKEN_ID": GITHUB_TOKEN_SECRET_ID,
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Variáveis de ambiente ausentes: {', '.join(missing_vars)}")

# Executa a validação quando o módulo é importado
validate_config()