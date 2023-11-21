from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
import pandas as pd 
import os
from pathlib import Path

# [START default_args]
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# [END default_args]
sql_f_createT_path = Path(os.environ['AIRFLOW_HOME'] + '/dags/sql/create_tables.sql')


@task(task_id="extract_and_transformation_data")
def extract_transform_data():
    # Extract data from various sources
    print("Data extracted successfully") # Return a serializable result
    df_urgence = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/raw/donnees-urgences-SOS-medecins.csv"),
                        sep=";",
                        dtype="unicode")
    df_urgence = df_urgence.drop(["nbre_acte_tot_f","nbre_acte_tot_h","nbre_acte_corona_f","nbre_acte_corona_h","nbre_acte_tot","nbre_acte_corona"], axis=1)
    print(df_urgence)

    df_tranche = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/raw/code-tranches-dage-donnees-urgences.csv"),
                        sep=";",
                        dtype="unicode")
    print(df_tranche)
    
    df_dep = pd.read_json(os.path.expandvars("${AIRFLOW_HOME}/data/raw/departements-region.json"))
    print(df_dep)

extract = extract_transform_data()


@task(task_id="load_data")
def load_data():
    print("Loading data into the database")

insert = load_data()

# [START instantiate_dag]
with DAG(
    'HealthDataWarehousing',
    default_args=default_args,
    description='ETL pipeline for health data warehousing',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['healthcare', 'data-warehousing'],
) as dag:

    start = BashOperator(
        task_id='Start',
        bash_command='echo Start',
    
    )

    create_tables = PostgresOperator(
        task_id='create_tables',
        sql=sql_f_createT_path.read_text(),
        postgres_conn_id='postgres_connexion', 
        autocommit=True,
    )

# Define the task dependencies
start >> create_tables >> extract >> insert