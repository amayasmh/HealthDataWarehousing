from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from sqlalchemy import create_engine
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

# Les chemins vers les fichiers sql pour creer les tables 
sql_f_createT_path = Path(os.environ['AIRFLOW_HOME'] + '/dags/sql/create_tables.sql')



@task(task_id="extract_and_transformation_data")
def extract_transform_data():

    # Lecture puis transformation des donnees "urgence" et stockage dans un fichier csv

    df_urgence = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/raw/donnees-urgences-SOS-medecins.csv"),
                        sep=";",
                        dtype="unicode",
                        )

    # Ajout d'une colonne id
    df_urgence['id'] = range(len(df_urgence))
    print(df_urgence)

    # Transformation + Suppression des valeurs nulles et stockage du fichier mesures
    df_urgence_mesures = df_urgence[["id", "dep", "date_de_passage", "sursaud_cl_age_corona", "nbre_pass_tot", "nbre_pass_corona", "nbre_hospit_corona"]].copy()   
    df_urgence_mesures["date_de_passage"] = pd.to_datetime(df_urgence_mesures["date_de_passage"], errors="coerce", format='%Y-%m-%d')
    df_urgence_mesures = df_urgence_mesures.rename({'sursaud_cl_age_corona': 'code_age'}, axis=1)
    df_urgence_mesures = df_urgence_mesures.dropna()
    df_urgence_mesures.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/donnees-urgence-mesures.csv"), index=False)
    print(df_urgence_mesures)

    
    # Transformation + Suppression des valeurs nulles et stockage du fichier hospit 
    df_urgence_h= df_urgence.loc[df_urgence["sursaud_cl_age_corona"] == "0"].copy()
    df_urgence_hospit = df_urgence_h[["id", "nbre_pass_tot_h", "nbre_pass_tot_f", "nbre_pass_corona_h", "nbre_pass_corona_f", "nbre_hospit_corona_h", "nbre_hospit_corona_f"]].copy()
    df_urgence_hospit = df_urgence_hospit.rename({'id': 'urgence_mesures_id'}, axis=1)
    df_urgence_hospit = df_urgence_hospit.dropna()
    df_urgence_hospit.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/donnees-urgence-hospit.csv"), index=False)  
    print(df_urgence_hospit)           



    # Lecture puis transformation des donnees "tranche d'ages" et stockage dans un fichier csv
    df_tranche = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/raw/code-tranches-dage-donnees-urgences.csv"),
                        sep=";",
                        dtype="unicode")
    df_tranche.rename(columns={' tranches d\'age': 'tranche', 'Code': 'id'}, inplace=True)
    df_tranche['tranche'] = df_tranche['tranche'].str.strip()
    df_tranche.at[0, 'tranche'] = 'tous ages'
    df_tranche.to_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/code-tranches-dage-donnees-urgences.csv"), index=False)
    print(df_tranche)

    # Lecture puis transformation des donnees "departement" et stockage dans un fichier csv
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
    df_urgence_mesures = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/donnees-urgence-mesures.csv"))
    df_urgence_hospit = pd.read_csv(os.path.expandvars("${AIRFLOW_HOME}/data/processed/donnees-urgence-hospit.csv"))
    
    # Insertion des donnees
    df_tranche.to_sql('tranche_age', con=engine, if_exists='append', index=False,  method='multi', chunksize=1000)
    df_dep.to_sql('departement', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    df_urgence_mesures.to_sql('urgence_mesures', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    df_urgence_hospit.to_sql('urgence_hospit', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)

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
extract >> create_tables >> [insert]