from Auxiliares_ext_quiver.passwords import QUIVER_PASSWORD,POSTGRESS_PASSWORD

# Data inicial de inclusão dos regsitros nas querys
date_start = '2022-01-01'

# Quiver Corporativo
QUIVER_USERNAME = 'bq_dwcorporativo_u'
QUIVER_PASSWORD = QUIVER_PASSWORD
QUIVER_DBNAME = 'dbQuiver_CGF'
QUIVER_HOST =  '10.13.72.11'
QUIVER_PORT = '50666'

# DW Corporativo
DW_CORPORATIVO_USERNAME = 'master'
DW_CORPORATIVO_PASSWORD = POSTGRESS_PASSWORD
DW_CORPORATIVO_DBNAME = 'postgres'
DW_CORPORATIVO_HOST = 'db-dw-prod.postgres.database.azure.com'
DW_CORPORATIVO_PORT = '5432'

# Schema para a criação e inserção de registros do ETL
SCHEMA_CRIACAO_INSERT = 'staging'