from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
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

@task(task_id="extract_data")
def extract_data():
    # Extract data from various sources
    print("Data extracted successfully") # Return a serializable result

extract = extract_data()

@task(task_id="transform_data")
def transform_data():
    # Transform data
    print("Transforming data")

transform = transform_data()

@task(task_id="create_tables")
def create_tables():
    print("Creating tables in the database")

create = create_tables()

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

# Define the task dependencies
start >> extract >> transform >> create >> insert