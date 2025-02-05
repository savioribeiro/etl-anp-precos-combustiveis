from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import sys
import os
from dotenv import load_dotenv

load_dotenv()

# Adicionar o caminho do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from extract import download_files, delete_existing_files
from transform import process_file
from load import load_to_postgres

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%d-%m-%Y %H:%M:%S')
logg = logging.getLogger(__name__)

FILE_LINKS = os.getenv("FILE_LINKS").split(',')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_anp_preco_combustiveis',
    default_args=default_args,
    description='ETL para dados de preços de combustíveis da ANP',
    start_date=datetime(2023, 10, 1),
    catchup=False,
)

def extract():
    try:
        delete_existing_files()
        logg.info("Iniciando a extração dos arquivos.")
        files = download_files(FILE_LINKS)
        logg.info("Extração concluída.")
        return files
    except Exception as e:
        logg.error(f"Erro na extração dos arquivos: {e}")
        raise

def transform(ti):
    try:
        files = ti.xcom_pull(task_ids='extract')
        logg.info("Iniciando a transformação dos arquivos.")
        results = [process_file(file) for file in files]
        logg.info("Transformação concluída.")
        return results
    except Exception as e:
        logg.error(f"Erro na transformação dos arquivos: {e}")
        raise

def load(ti):
    try:
        results = ti.xcom_pull(task_ids='transform')
        logg.info("Iniciando a carga dos dados no BD.")
        for i, (df, filepath) in enumerate(results):
            logg.info(f"Carregando o df {i+1} com {len(df)} linhas.")
            load_to_postgres(df)
        logg.info("Carga concluída.")
    except Exception as e:
        logg.error(f"Erro na carga dos dados: {e}")
        raise

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task
