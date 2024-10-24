from datetime import timedelta, datetime

import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from sub_dag.subdag import get_product_all_pages, save_crawled_data_to_csv, load_csv_to_mysql

DAG_NAME = 'etl_project'
SCHEDULE_INTERVAL = "00 01 * * *"

args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 23),
    'retries': 1,
    #"on_failure_callback": slack_failure_callback,
}

with DAG(
        dag_id=DAG_NAME,
        default_args=args,
        schedule='@daily',
        tags=["ETL_PROJECT"]
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    crawl = PythonOperator(
        task_id="crawl",
        python_callable=get_product_all_pages,
        provide_context=True
    )

    load_to_csv = PythonOperator(
        task_id="load_data_to_csv",
        python_callable=save_crawled_data_to_csv,
        provide_context=True
    )

    load_csv_to_mysql = PythonOperator(
        task_id="load_csv_to_mysql",
        python_callable=load_csv_to_mysql,
        provide_context=True
    )


    # load_stg = task_load_staging(
    #     group_id="load_to_stg",
    #     args=args,
    #     **kwargs
    # )
    # done_load_stg = EmptyOperator(task_id="done_load_stg")
    # load_dwh = task_load_wh_dag(
    #     group_id="load_to_dwh",
    #     args=args,
    #     **kwargs
    # )

    start_pipeline >> crawl >> load_to_csv >> load_csv_to_mysql >> end_pipeline
