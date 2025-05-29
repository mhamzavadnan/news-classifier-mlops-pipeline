from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import kagglehub  # keep if you need it in your callable


def fetch_and_print_kaggle_vars():
    kaggle_username = Variable.get("kaggle_username")
    kaggle_api_key = Variable.get("kaggle_api_key")

    print(f"Kaggle Username: {kaggle_username}")
    print(f"Kaggle API Key: {kaggle_api_key}")


with DAG(
    dag_id="kaggle_vars_fetcher",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_task = PythonOperator(
        task_id="print_kaggle_vars",
        python_callable=fetch_and_print_kaggle_vars,
    )
