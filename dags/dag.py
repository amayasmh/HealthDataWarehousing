from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
import pandas as pd 
import os
from pathlib import Path
from sqlalchemy import create_engine


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

# Les chemins vers les fichiers sql
sql_f_createT_path = Path(os.environ['AIRFLOW_HOME'] + '/dags/sql/create_tables.sql')
# sql_insere_file_path = Path(os.environ['AIRFLOW_HOME'] + '/dags/sql/insert_values.sql')


@task(task_id="extract_and_transformation_data")
def extract_transform_data():

    #Lecture puis transformation des donnees "urgence" et stockage dans un fichier csv

    df_urgence = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/raw/donnees-urgences-SOS-medecins.csv"),
                        sep=";",
                        dtype="unicode")
    df_urgence = df_urgence.drop(["nbre_acte_tot_f","nbre_acte_tot_h","nbre_acte_corona_f","nbre_acte_corona_h","nbre_acte_tot","nbre_acte_corona"], axis=1)
    df_urgence["date_de_passage"] = pd.to_datetime(df_urgence["date_de_passage"],errors = "coerce",format='mixed')
    df_urgence['date_de_passage'] = df_urgence['date_de_passage'].dt.strftime('%Y-%m-%d')
    df_urgence.columns.values[2] = 'code_age'
    df_urgence.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/donnees-urgences-SOS-medecins.csv"), index=False)
    print(df_urgence)

    #Lecture puis transformation des donnees "tranche d'ages" et stockage dans un fichier csv

    df_tranche = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/raw/code-tranches-dage-donnees-urgences.csv"),
                        sep=";",
                        dtype="unicode")
    df_tranche.rename(columns={' tranches d\'age': 'tranche', 'Code': 'id'}, inplace=True)
    df_tranche['tranche'] = df_tranche['tranche'].str.strip()
    df_tranche.at[0, 'tranche'] = 'tous ages'
    df_tranche.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/code-tranches-dage-donnees-urgences.csv"), index=False)
    print(df_tranche)

    #Lecture puis transformation des donnees "departement" et stockage dans un fichier csv

    df_dep = pd.read_json(os.path.expandvars("${AIRFLOW_HOME}/data/raw/departements-region.json"))
    df_dep.rename(columns={'num_dep': 'id_dep', 'dep_name' : 'nom_dep' , 'region_name':'region'}, inplace=True)
    df_dep.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/departements-region.csv"), index=False)
    print(df_dep)


    
extract = extract_transform_data()


@task(task_id="load_data")
def load_data():

    # Parametre de connexion
    utilisateur = "airflow"
    mot_de_passe = "airflow"
    host = "172.17.0.1:5435"
    base_de_donnees = "postgres"

    # Création de la connexion à la base de données
    engine = create_engine(f'postgresql://{utilisateur}:{mot_de_passe}@{host}/{base_de_donnees}')
    
    # Chargement des donnees depuis les fichiers
    df_tranche = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/code-tranches-dage-donnees-urgences.csv"))
    df_dep = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/departements-region.csv"))
    df_urgence = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/donnees-urgences-SOS-medecins.csv"))
    
    # Insertion des donnees
    df_tranche.to_sql('tranche_age', con=engine, if_exists='append', index=False,  method='multi', chunksize=1000)
    df_dep.to_sql('departement', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    df_urgence.to_sql('urgence', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)

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

    # Creation des tables en executant le fichier sql
    create_tables = PostgresOperator(
        task_id='create_tables',
        sql=sql_f_createT_path.read_text(),
        postgres_conn_id='postgres_connexion', 
        autocommit=True,
    )

# Define the task dependencies
create_tables >> extract >> [insert]