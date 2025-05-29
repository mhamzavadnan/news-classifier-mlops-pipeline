# from airflow import DAG
# from airflow.exceptions import AirflowException
# from airflow.sdk import Variable, task
# from datetime import datetime
# import os
# import kagglehub

# with DAG(
#     dag_id="news_data_pipeline",
#     start_date=datetime(2023, 1, 1),
#     schedule="@daily",
# ) as dag:
#     @task(task_id="fetch_news_data")
#     def fetch_news_data(**context):
#         try:
#             os.environ['KAGGLE_USERNAME'] = Variable.get("KAGGLE_USERNAME")
#             os.environ['KAGGLE_KEY'] = Variable.get("KAGGLE_KEY")
            
#             exec_date = context['ds']
#             download_path = f"D:mlops-project/data/raw/{exec_date}"
#             os.makedirs(download_path, exist_ok=True)
            
#             path = kagglehub.dataset_download(
#                 "rmisra/news-category-dataset",
#                 path=download_path
#             )
            
#             return {'dataset_path': path, 'execution_date': exec_date}
            
#         except Exception as e:
#             raise AirflowException(f"Failed to download dataset: {str(e)}")
    
#     fetch_task = fetch_news_data()

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import os

with DAG(
    dag_id="news_data_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@daily"
) as dag:

    @task(task_id='fetch_news_dataset')
    def fetch_news_data(**context):
        try:
            os.environ["KAGGLE_USERNAME"] = Variable.get("KAGGLE_USERNAME")
            os.environ["KAGGLE_KEY"] = Variable.get("KAGGLE_KEY")

            import kaggle
            kaggle.api.authenticate()
            
            dataset = "rmisra/news-category-dataset"
            exec_date = context['ds']
            download_path = f"data/raw/{exec_date}"
            
            os.makedirs(download_path, exist_ok=True)
            
            kaggle.api.dataset_download_files(
                dataset=dataset,
                path=download_path,
                unzip=True
            )
            
            return {'status': 'success', 'path': download_path}
            
        except Exception as e:
            raise AirflowException(f"Dataset download failed. Verify: {dataset} exists and is public. Error: {str(e)}")
        
    fetch_task = fetch_news_data()