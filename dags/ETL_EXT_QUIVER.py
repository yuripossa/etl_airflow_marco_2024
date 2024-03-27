from Auxiliares_ext_quiver.variables import (QUIVER_USERNAME, QUIVER_PASSWORD, QUIVER_DBNAME, QUIVER_HOST, QUIVER_PORT, DW_CORPORATIVO_USERNAME, DW_CORPORATIVO_PASSWORD, DW_CORPORATIVO_DBNAME, DW_CORPORATIVO_HOST, DW_CORPORATIVO_PORT)
from Auxiliares_ext_quiver.querys import query_quiver_clientes, deleteQuiverClientes
from Auxiliares_ext_quiver.my_functions_quiver import get_data_from_db, PostgreSQL_Insert, PSSql_Delete
import pytz
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Conexão Quiver Clientes
def Query_quiver_clientes():
    try:
        data = get_data_from_db("Mssql", QUIVER_USERNAME, QUIVER_PASSWORD, QUIVER_DBNAME, QUIVER_HOST, QUIVER_PORT, query_quiver_clientes)        
    except Exception as e:
        print("Não foi possível acessar o banco.")
        data = None
    return data

# Deletes
def Delete_quiver_clientes():
    PSSql_Delete("Postgres", DW_CORPORATIVO_USERNAME, DW_CORPORATIVO_PASSWORD, DW_CORPORATIVO_DBNAME, DW_CORPORATIVO_HOST, DW_CORPORATIVO_PORT, deleteQuiverClientes)

# Inserts
def Insert_quiver_clientes():
    dataclientes = Query_quiver_clientes()
    if dataclientes is not None:
        PostgreSQL_Insert(DW_CORPORATIVO_USERNAME, DW_CORPORATIVO_PASSWORD, DW_CORPORATIVO_DBNAME, DW_CORPORATIVO_HOST, dataclientes, 'staging.ext_clientes_quiver')
    else:
        print("Não foi possível realizar a inserção. Os dados estão vazios.")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

# Configurando o fuso horário
sp_timezone = pytz.timezone('America/Sao_Paulo')

# Definindo a data e hora desejada
start_date = sp_timezone.localize(datetime(2024, 3, 6, 7, 45))

with DAG(
    'ETL_EXT_QUIVER',
    default_args=default_args,
    description='DAG para ETL de dados QUIVER',
    schedule_interval='45 0,11,17 * * *',
    start_date=start_date,
    catchup=False,
) as dag:
    del_clientes_quiver = PythonOperator(task_id="DELETE_CLIENTES_QUIVER", python_callable=Delete_quiver_clientes)
    ext_clientes_quiver = PythonOperator(task_id="SELECT_CLIENTES_QUIVER", python_callable=Query_quiver_clientes)
    insert_clientes_quiver = PythonOperator(task_id="INSERT_CLIENTES_QUIVER", python_callable=Insert_quiver_clientes)

    # Estrutura das tarefas
    del_clientes_quiver >> ext_clientes_quiver >> insert_clientes_quiver
