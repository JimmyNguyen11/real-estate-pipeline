from airflow.hooks.base import BaseHook
import json
from pyhive import hive


def get_spark_thrift_conn(hive_server2_conn_id: str = "hiveserver2_default_1") -> hive.Connection:
    """
    Please close conn after using or use with () statement for auto-closing!
    """
    conn_query_params = BaseHook.get_connection(hive_server2_conn_id)

    # print(type(conn_query_params.get_extra()), conn_query_params.get_extra())

    # auth: "NOSASL"/"LDAP"
    #extra = json.loads(conn_query_params.get_extra() or "{}")
    #password = conn_query_params.password if conn_query_params.login else None
    conn = hive.connect(
        host=conn_query_params.host,
        database=conn_query_params.schema,
        port=conn_query_params.port,
        username=conn_query_params.login,
        password=None,
        auth="NONE"
    )
    print("NOTICE: Please close conn after using or use with () statement for auto-closing!")
    return conn