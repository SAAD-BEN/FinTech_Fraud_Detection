from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from loading import insert_customers_into_hive, insert_transactions_into_hive, insert_external_data_into_hive

# Define your API endpoints
customers_endpoint = "http://127.0.0.1:5000/customers"
transactions_endpoint = "http://127.0.0.1:5000/transactions"
external_data_endpoint = "http://127.0.0.1:5000/external_data"

# Function to fetch data from an endpoint
def fetch_data(endpoint):
    try:
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {endpoint}: {e}")
        return None

# Function to insert customers into Hive
def insert_customers(**kwargs):
    customers_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_data_customers')
    insert_customers_into_hive("finaldb", "customers", customers_data)

# Function to insert transactions into Hive
def insert_transactions(**kwargs):
    transactions_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_data_transactions')
    insert_transactions_into_hive("finaldb", "transactions", transactions_data)

# Function to insert external data into Hive
def insert_external_data(**kwargs):
    external_data = kwargs['task_instance'].xcom_pull(task_ids='fetch_data_external_data')
    insert_external_data_into_hive("finaldb", "cust_external_data", "blacklist", external_data)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my_airflow_dag',
    default_args=default_args,
    description='My Airflow DAG for data loading',
    schedule_interval=timedelta(minutes=20),
)

# Task to fetch customers data from API
fetch_customers_task = PythonOperator(
    task_id='fetch_data_customers',
    python_callable=fetch_data,
    op_kwargs={'endpoint': customers_endpoint},
    provide_context=True,
    dag=dag,
)

# Task to fetch transactions data from API
fetch_transactions_task = PythonOperator(
    task_id='fetch_data_transactions',
    python_callable=fetch_data,
    op_kwargs={'endpoint': transactions_endpoint},
    provide_context=True,
    dag=dag,
)

# Task to fetch external data from API
fetch_external_data_task = PythonOperator(
    task_id='fetch_data_external_data',
    python_callable=fetch_data,
    op_kwargs={'endpoint': external_data_endpoint},
    provide_context=True,
    dag=dag,
)

# Tasks to insert data into Hive tables
insert_customers_task = PythonOperator(
    task_id='insert_customers',
    python_callable=insert_customers,
    provide_context=True,
    dag=dag,
)

insert_transactions_task = PythonOperator(
    task_id='insert_transactions',
    python_callable=insert_transactions,
    provide_context=True,
    dag=dag,
)

insert_external_data_task = PythonOperator(
    task_id='insert_external_data',
    python_callable=insert_external_data,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
[fetch_customers_task, fetch_transactions_task, fetch_external_data_task] >> [insert_customers_task, insert_transactions_task, insert_external_data_task]

if __name__ == "__main__":
    dag.cli()
