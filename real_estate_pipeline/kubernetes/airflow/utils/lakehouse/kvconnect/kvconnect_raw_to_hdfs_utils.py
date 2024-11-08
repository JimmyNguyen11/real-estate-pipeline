import os
import sys

abs_path = os.path.dirname(os.path.abspath(__file__)) + '/../../..'
sys.path.append(abs_path)

from airflow.example_dags.utils.lakehouse.lakehouse_layer_utils import RAW, BUCKET_WAREHOUSE, ICEBERG, PARQUET_FORMAT
from airflow.hooks.base_hook import BaseHook
from airflow.example_dags.schema.common.dao_dim import DaoDim
from airflow.example_dags.schema.common.model import BaseModel

SQL_DIR = f'{abs_path}/sql/lakehouse/kvconnect'
HDFS_CONN_ID = 'hdfs_connection_default'
HIVE_CONN_ID = 'hiveserver2_default_1'
RAW_CONN_ID = 'mysql_kdn_core_id'


class BaseKvConnectUtil:
    def __init__(self, table_name_raw: str, table_name: str, hdfs_conn_id: str):
        self.table_name_raw = table_name_raw
        self.table_name = table_name
        self.hdfs_conn_id = hdfs_conn_id
        conn = BaseHook.get_connection(self.hdfs_conn_id)
        self.host = conn.host
        self.port = str(conn.port)

    def get_sql_path_select_all(self, sql_dir: str = SQL_DIR) -> str:
        return f'{sql_dir}/select_{self.table_name_raw}.sql'

    def get_content_from_sql_path(self, sql_path: str) -> str:
        data = ''
        try:
            with open(sql_path, 'r') as file:
                data = file.read()
        except:
            print(f'Loi doc file sql {sql_path}')
        return data


class BaseKvConnectModel(DaoDim, BaseModel):
    def __init__(self, table_name_raw: str, table_name_warehouse: str, columns: list, is_write_truncate: bool = True,
                 table_type: str = None
                 , hdfs_conn_id=None, raw_conn_id=None, hive_server2_conn_id=None
                 , bucket: str = BUCKET_WAREHOUSE, source_file_format: str = PARQUET_FORMAT
                 , raw_pandas_schema: dict = {}, raw_pyarrow_schema: dict = {}
                 , sql: str = None, having_staging: bool = False, sort_column_expr: str = ""
                 , key_columns: list = []
                 ):
        super().__init__(table_name_warehouse)
        self.table_name_raw = table_name_raw
        self.table_name_warehouse = table_name_warehouse
        self.bucket = bucket
        self.COLUMNS = columns
        self.IS_WRITE_TRUNCATE = is_write_truncate
        self.TABLE_TYPE = table_type
        self.COLUMNS_SCHEMA = self.COLUMNS
        self.iceberg_db = f'{ICEBERG}.{bucket}'
        self.source_file_format = source_file_format
        self.raw_pandas_schema = raw_pandas_schema
        self.raw_pyarrow_schema = raw_pyarrow_schema
        self.hdfs_conn_id = hdfs_conn_id
        self.raw_conn_id = raw_conn_id
        self.hive_server2_conn_id = hive_server2_conn_id
        self.mys = None  # BaseKvConnectUtil
        self.sql = sql
        self.having_staging = having_staging
        # sort_column_expr, ex: ORDER BY date, using for case "partitioned by"
        self.sort_column_expr = sort_column_expr
        self.KEY_COLUMNS = key_columns

    def set_hdfs_conn_id(self, hdfs_conn_id):
        if hdfs_conn_id:
            self.hdfs_conn_id = hdfs_conn_id
            self.mys = BaseKvConnectUtil(self.table_name_raw, self.TABLE_NAME, hdfs_conn_id)

    def set_raw_conn_id(self, raw_conn_id):
        if raw_conn_id:
            self.raw_conn_id = raw_conn_id

    def set_hive_server2_conn_id(self, hive_server2_conn_id):
        if hive_server2_conn_id:
            self.hive_server2_conn_id = hive_server2_conn_id

    def set_conn_id(self, hdfs_conn_id, raw_conn_id, hive_server2_conn_id):
        self.set_hdfs_conn_id(hdfs_conn_id)
        self.set_raw_conn_id(raw_conn_id)
        self.set_hive_server2_conn_id(hive_server2_conn_id)

    def get_mys(self):
        return self.mys

    def get_raw_pandas_schema(self):
        return self.raw_pandas_schema

    def set_raw_pandas_schema(self, raw_pandas_schema: dict = {}):
        self.raw_pandas_schema = raw_pandas_schema

    def get_raw_pyarrow_schema(self):
        return self.raw_pyarrow_schema

    def set_raw_pyarrow_schema(self, raw_pyarrow_schema: dict = {}):
        self.raw_pyarrow_schema = raw_pyarrow_schema


class MysqlBaseModel():
    def __init__(self, table_name_raw: str = None, schema: dict = {}) -> None:
        self.table_name_raw = table_name_raw
        self.schema = schema

    def get_table_name_raw(self):
        return self.table_name_raw

    def get_schema(self):
        return self.schema
