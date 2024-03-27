from Auxiliares_ext_clientes_nbs.variables import(NBS_USERNAME,NBS_PASSWORD,NBS_DBNAME,NBS_HOST,NBS_PORT,date_start,SCHEMA_CRIACAO_INSERT,DW_CORPORATIVO_USERNAME,
DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,DW_CORPORATIVO_PORT)
from Auxiliares_ext_clientes_nbs.querys import (query_nbs_clientes, query_nbs_faturamento, query_nbs_financeiro,deleteNbsClientes,deleteNbsFaturamento, deleteNbsFinanceiro)
from Auxiliares_ext_clientes_nbs.my_functions_nbs import (get_data_from_db,PostgreSQL_Insert,PSSql_Delete)
import pytz
import pandas as pd
from datetime import datetime, timedelta
#import pymssql 
from airflow import DAG
from airflow.operators.python import PythonOperator

#region Conexão NBS Clientes
def Query_nbs_clientes():
    try:
        data = get_data_from_db("Oracle", NBS_USERNAME, NBS_PASSWORD, NBS_DBNAME, NBS_HOST, NBS_PORT, query_nbs_clientes)        
       
    except Exception as e:
        print("Não foi possivel acessar o banco.")
        data = None
    return data
#endregion

#region Conexão NBS Financeiro
def Query_nbs_financeiro():
    try:
        data = get_data_from_db("Oracle", NBS_USERNAME, NBS_PASSWORD, NBS_DBNAME, NBS_HOST, NBS_PORT, query_nbs_financeiro)

    except Exception as e:
        print("Não foi possível acessar o banco.")
        data = None
    return data
#endregion

#region Conexão NBS Faturamento
def Query_nbs_faturamento():
    try:
        data = get_data_from_db("Oracle", NBS_USERNAME, NBS_PASSWORD, NBS_DBNAME, NBS_HOST, NBS_PORT, query_nbs_faturamento)       
        
    except Exception as e:
        print("Não foi possivel acessar o banco Oracle.")
        data = None
    return data
#endregion

#region Deletes
def delete_nbs_clientes():
    PSSql_Delete("Postgres",DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,DW_CORPORATIVO_PORT,deleteNbsClientes)

def delete_nbs_financeiro():
    PSSql_Delete("Posgtres",DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,DW_CORPORATIVO_PORT,deleteNbsFinanceiro)

def delete_nbs_faturamento():
    PSSql_Delete("Postgres",DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,DW_CORPORATIVO_PORT,deleteNbsFaturamento)
#endregion

#region Inserts
def Insert_nbs_clientes():
    dataclientes = Query_nbs_clientes()
    PostgreSQL_Insert(DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,dataclientes,'staging.ext_clientes_nbs')

def Insert_nbs_financeiro():
    datafinanceiro = Query_nbs_financeiro()
    PostgreSQL_Insert(DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,datafinanceiro,'staging.ext_financeiro_nbs')

def Insert_nbs_faturamento():
    datafaturamento = Query_nbs_faturamento()
    PostgreSQL_Insert(DW_CORPORATIVO_USERNAME,DW_CORPORATIVO_PASSWORD,DW_CORPORATIVO_DBNAME,DW_CORPORATIVO_HOST,datafaturamento,'staging.ext_faturamento_nbs')
#endregion

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Configurando o fuso horário
sp_timezone = pytz.timezone('America/Sao_Paulo')

# Definindo a data e hora desejada
start_date = sp_timezone.localize(datetime(2024, 3, 6, 7, 45))

with DAG(
    'ETL_EXT_NBS',
    default_args=default_args,
    description='DAG para ETL de dados NBS',
    schedule_interval='45 0,11,17 * * *',
    start_date=start_date,
    catchup=False,
) as dag:
        ext_clientes_nbs = PythonOperator(task_id = "SELECT_CLIENTES_NBS",python_callable=Query_nbs_clientes)
        ext_financeiro_nbs = PythonOperator(task_id = "SELECT_FINANCEIRO_NBS",python_callable=Query_nbs_financeiro)
        ext_faturamento_nbs = PythonOperator(task_id = "SELECT_FATURAMENTO_NBS",python_callable=Query_nbs_faturamento)
        
        insert_clientes_nbs = PythonOperator(task_id = "INSERT_CLIENTES_NBS",python_callable=Insert_nbs_clientes)
        insert_financeiro_nbs = PythonOperator(task_id = "INSERT_FINANCEIRO_NBS",python_callable=Insert_nbs_financeiro)
        insert_faturamento_nbs = PythonOperator(task_id = "INSERT_FATURAMENTO_NBS", python_callable=Insert_nbs_faturamento)

        del_clientes_nbs = PythonOperator(task_id = "DEL_CLIENTES_NBS",python_callable=delete_nbs_clientes)
        del_faturamento_nbs = PythonOperator(task_id = "DEL_FATURAMENTO_NBS",python_callable=delete_nbs_faturamento)
        del_financeiro_nbs = PythonOperator(task_id = "DEL_FINANCEIRO_NBS", python_callable=delete_nbs_financeiro)

 # Estrutura das tarefas
del_clientes_nbs >> ext_clientes_nbs >> insert_clientes_nbs
del_faturamento_nbs >> ext_faturamento_nbs >> insert_faturamento_nbs
del_financeiro_nbs >> ext_financeiro_nbs >> insert_financeiro_nbs
