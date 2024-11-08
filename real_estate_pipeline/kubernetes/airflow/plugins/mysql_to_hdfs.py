from airflow.models import BaseOperator
from airflow.example_dags.plugins.hook.pyarrow_hdfs_hook import PyarrowHdfsHook
from airflow.utils.decorators import apply_defaults
import pandas as pd
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.mysql_hook import MySqlHook
import pyarrow.parquet as pq
from airflow.example_dags.utils.database.lakehouse_mapping_dtypes import get_pyarrow_table
from airflow.example_dags.utils.hdfs.hdfs_utils import run_bash_cmd
from airflow.hooks.base import BaseHook
import mysql.connector
import os
import sys
import subprocess

DEFAULT_CHUNK_SIZE = 500000


class MysqlToHdfsOperator(BaseOperator):
    template_fields = (
        'query', 'raw_pandas_schema', 'raw_pyarrow_schema', 'mysql_conn_id', 'hdfs_conn_id', 'output_path', 'is_truncate'
    )
    template_ext = (".sql",)
    ui_color = "#e4f0e8"

    @apply_defaults
    def __init__(self,
                 mysql_conn_id,
                 hdfs_conn_id,
                 query,
                 raw_pandas_schema,
                 raw_pyarrow_schema,
                 output_path,
                 table_name,
                 is_log_schema=False,
                 is_truncate=False,
                 *args, **kwargs):
        super(MysqlToHdfsOperator, self).__init__(*args, **kwargs)
        self.mysql_conn_id = mysql_conn_id
        self.hdfs_conn_id = hdfs_conn_id
        self.query = query
        self.output_path = output_path
        self.table_name = table_name
        self.raw_pandas_schema = raw_pandas_schema
        self.raw_pyarrow_schema = raw_pyarrow_schema
        self.chunk_size = DEFAULT_CHUNK_SIZE
        self.is_log_schema = is_log_schema
        self.is_truncate = is_truncate

    def execute(self, context):
        #mysql_hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)

        # Đọc dữ liệu từ MySQL
        #db_conn = mysql_hook.get_conn()

        mysql_conn = BaseHook.get_connection('mysql_conn_id')

        # Kết nối MySQL
        db_conn = mysql.connector.connect(
            host=mysql_conn.host,
            user=mysql_conn.login,
            password=mysql_conn.password,
            database=mysql_conn.schema
        )
        cursor = db_conn.cursor()

        hdfs_hook = PyarrowHdfsHook(self.hdfs_conn_id)
        hdfs_conn = hdfs_hook.get_conn()

        self.log.info(f'Hdfs raw path: {self.output_path}')
        self.log.info(f'Query raw: \n{self.query}')

        # truncate dir
        if self.is_truncate:
            cmd_mkdir_output_path = f"hdfs dfs -rm -r -f {self.output_path}"
            run_bash_cmd(command=cmd_mkdir_output_path)

        # create dir
        cmd_mkdir_output_path = f"hdfs dfs -mkdir -p {self.output_path}" # /demo/raw/demo/11-01-2024
        run_bash_cmd(command=cmd_mkdir_output_path)
        cmd_allow_permission = f"hdfs dfs -chown -R admin:admin {self.output_path}"
        run_bash_cmd(command=cmd_allow_permission)

        # firstly, casting schema pandas for non string columns
        # secondly, infer schema when creating pyarrow table to convert None -> Null for string type
        self.log.info("getting data from database")
        for df in pd.read_sql(self.query, db_conn, chunksize=self.chunk_size):
            if self.is_log_schema:
                print('Schema truoc khi cast')
                print(df.dtypes)

            self.log.info('Casting schema pandas dataframe')
            df_cast_type = df.astype(self.raw_pandas_schema, errors="ignore")

            if self.is_log_schema:
                print(f'Schema sau khi cast:')
                print(df_cast_type.dtypes)

            self.log.info("Converting pandas dataframe to pyarrow table with pyarrow schema casting")
            table = get_pyarrow_table(
                pandas_df=df_cast_type,
                pyarrow_schema=self.raw_pyarrow_schema
            )
            self.log.info('Write file parquet to HDFS')
            pq.write_to_dataset(table, root_path=self.output_path, filesystem=hdfs_conn)

        # close source database connection to release resource
        db_conn.close()

        list_files_command = f"hdfs dfs -ls {self.output_path} | grep '^-' | sort -k6,7 | tail -1 | awk '{{print $8}}'"
        filename = subprocess.check_output(list_files_command, shell=True, text=True).strip()
        if filename:
            # Đổi tên file
            rename_command = f"hdfs dfs -mv {filename} {self.output_path}/{self.table_name}_0.parquet"
            run_bash_cmd(command=rename_command)
            print(f"File {filename} đã được đổi tên thành {self.output_path}/{self.table_name}_0.parquet")
        else:
            print("Không tìm thấy file nào trong thư mục.")



class MysqlToHdfsPlugin(AirflowPlugin):
    name = "mysql_to_hdfs_plugin"
    operators = [MysqlToHdfsOperator]

#hadoop fs -put crawled_data.csv /demo/raw
