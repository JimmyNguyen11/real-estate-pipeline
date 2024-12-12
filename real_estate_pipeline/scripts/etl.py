from tokenize import endpats

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from airflow.example_dags.utils.hdfs.hdfs_utils import run_bash_cmd
from datetime import timedelta
from airflow.example_dags.utils.variables.variables_utils import get_variables
from airflow.example_dags.utils.common.common_function import get_param

from datetime import datetime
from airflow.example_dags.utils.date_time.date_time_utils import get_business_date, get_partition_time
from airflow.operators.empty import EmptyOperator
from airflow.example_dags.subdags.test.sub_dag_1 import task_load_to_hdfs, load_csv_to_mysql, get_product_all_pages, save_crawled_data_to_csv, task_load_stg, task_load_dwh

#from plugins.iceberg_operator import IcebergOperator
#from plugins.source_file_to_iceberg_operator import SourceFileToIcebergOperator
from airflow.example_dags.plugins.mysql_to_hdfs import MysqlToHdfsOperator
#from schema.lakehouse.kms.schema_dlk import TableKmsDLK
#from schema.lakehouse.kms.schema_dwh import TableKmsDWH
from airflow.example_dags.schema.test.schema_dlk import TableTestDLK
from airflow.example_dags.utils.database.lakehouse_mapping_dtypes import get_raw_columns, get_type_pyarrow_from_pandas
from airflow.example_dags.utils.lakehouse.lakehouse_layer_utils import RAW, STAGING, WAREHOUSE
from airflow.example_dags.utils.lakehouse.lakehouse_uri_utils import get_source_uri_lakehouse



DAG_NAME = 'etl_4'
SCHEDULE_INTERVAL = "00 01 * * *"

product = "bds4"
hdfs_bucket = "/demo4"
mysql_conn_id = "mysql_conn_id"
hdfs_conn_id = "hdfs_connection_default"
business_date = get_business_date(days=-1, business_date=None)
date_partition = get_partition_time(business_date)
from_date = get_business_date(days=-1, business_date=None, date_format="%Y-%m-%d")
to_date = get_business_date(days=-1, business_date=None, date_format="%Y-%m-%d")

args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 1),
    'retries': 1
}

with DAG(
        dag_id=DAG_NAME,
        default_args=args,
        schedule='@daily',
        max_active_runs=1,
        tags=["hoang"]
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    '''crawl = PythonOperator(
        task_id="crawl",
        python_callable=get_product_all_pages,
        provide_context=True
    )'''

    '''load_to_csv = PythonOperator(
        task_id="load_data_to_csv",
        python_callable=save_crawled_data_to_csv,
        provide_context=True
    )'''

    kwargs = {
        "hdfs_bucket": hdfs_bucket,
        "mysql_conn_id": mysql_conn_id,
        "hdfs_conn_id": hdfs_conn_id,
        "from_date": from_date,
        "to_date": to_date,
        "date_partition": date_partition,
        "product": product
    }

    load_csv_to_mysql = PythonOperator(
        task_id="load_csv_to_mysql",
        python_callable=load_csv_to_mysql,
        provide_context=True
    )

    load_to_hdfs = task_load_to_hdfs(
        group_id="load_to_hdfs",
        args=args,
        **kwargs
    )
    
    load_stg = task_load_stg(
        group_id="load_to_stg",
        args=args,
        **kwargs
    )
    
    load_dwh = task_load_dwh(
        group_id="load_to_dwh",
        args=args,
        **kwargs
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

    #start_pipeline >> crawl >> load_to_csv >> load_csv_to_mysql >> load_to_hdfs >> load_stg >> load_dwh >> end_pipeline
    # SKIP task crawl vi mat nhieu time crawl, du lieu crawl tam thoi se luu trong file crawl_data_1.csv
    start_pipeline >> load_csv_to_mysql >> load_to_hdfs >> load_stg >> load_dwh >> end_pipeline  
    
