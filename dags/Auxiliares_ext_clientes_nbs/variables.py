from Auxiliares_ext_clientes_nbs.passwords import NBS_PASSWORD,POSTGRESS_PASSWORD

# Data inicial de inclusão dos regsitros nas querys
date_start = '2022-01-01'

# NBS Corporativo
NBS_USERNAME = 'NBS'
NBS_PASSWORD = NBS_PASSWORD
NBS_DBNAME = 'nbs.privatesubnet.natvcn.oraclevcn.com'
NBS_HOST =  '10.12.72.10'
NBS_PORT = '1521'

# DW Corporativo
DW_CORPORATIVO_USERNAME = 'master'
DW_CORPORATIVO_PASSWORD = POSTGRESS_PASSWORD
DW_CORPORATIVO_DBNAME = 'postgres'
DW_CORPORATIVO_HOST = 'db-dw-prod.postgres.database.azure.com'
DW_CORPORATIVO_PORT = '5432'

# Schema para a criação e inserção de registros do ETL
SCHEMA_CRIACAO_INSERT = 'staging'