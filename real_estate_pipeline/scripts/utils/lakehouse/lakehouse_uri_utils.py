import array
import os
import sys
import pandas as pd
from typing import List

abs_path = os.path.dirname(os.path.abspath(__file__)) + "/../../.."
sys.path.append(abs_path)
from airflow.example_dags.utils.lakehouse.lakehouse_layer_utils import RAW
from airflow.example_dags.utils.lakehouse.kvconnect.kvconnect_raw_to_hdfs_utils import BaseKvConnectUtil

BRACES = "{}"
ASTERISK = "*"


def generate_file_name_lakehouse(
        layer, table_folder, partition, table_name, data_format, gc_bucket=None, chunk=None
):
    # generate gc_bucket with postfix '/'
    gc_bucket = f"{gc_bucket}/" if gc_bucket else ""

    # generate chunk with postfix '_'
    chunk = f"{chunk}_" if chunk else ""

    # generate and return file_uri
    file_uri = f"{gc_bucket}{layer}/{table_folder}/{partition}/{table_name}_{chunk}{BRACES}.{data_format}"
    return file_uri


def get_source_uri_lakehouse(
        layer,
        table_folder,
        partition=None,
        data_prefix=None,
        data_format=None,
        gc_bucket=None,
        abs_uri=False,
):
    # generate gc_bucket with postfix '/' and prefix "/"
    if gc_bucket:
        gc_bucket = f"/{gc_bucket}" if gc_bucket[0] != "/" else gc_bucket
        gc_bucket = f"{gc_bucket}/" if gc_bucket[-1] != "/" else gc_bucket
    else:
        gc_bucket = ""

    # add hdfs:// to the beginning of gc_bucket to generate absolute uri
    gc_bucket = f"hdfs://{gc_bucket}" if (abs_uri and gc_bucket) else gc_bucket

    # generate partition with prefix '/'
    partition = f"/{partition}" if partition else ""

    # generate data_prefix with postfix '_' and prefix '/'
    data_prefix = f"/{data_prefix}_" if data_prefix else ""

    # generate data_format with prefix '*.'
    data_format = f"{ASTERISK}.{data_format}" if data_format else ""
    # append prefix '/' to data_format
    data_format = (
        f"/{data_format}" if (data_format and not data_prefix) else data_format
    )

    # generate and return source_uri
    source_uri = (
        f"{gc_bucket}{layer}/{table_folder}{partition}{data_prefix}{data_format}"
    )
    return source_uri


def get_source_uri_cdc(dimentions_table, topic, gc_bucket=None):
    """
    return uri for spark reading path from gcs (str)
    # FORMAT: /gc_bucket/dimentions_table/topic

    :type gc_bucket: str
    :type dimentions_table: str
    :type topic: str
    """
    uri_gc_bucket = f"gs://{gc_bucket}"
    source_uri = f"{uri_gc_bucket}/{dimentions_table}/{topic}"
    return source_uri


def get_source_regex_uri_cdc(
        dimentions_table, topic, lst_yyyymmdd, gc_bucket=None, data_format=None
):
    """
    return uri with regex parsing from lst_yyyymmdd for spark reading path from gcs
    # FORMAT: /gc_bucket/dimentions_table/topic/partition

    :type dimentions_table: str
    :type topic: str
    :type lst_yyyymmdd: lst of str
    :type gc_bucket: str
    :type data_format: str
    """
    all_yyyy = []
    all_mm = []
    all_dd = []
    for yyyymmdd in lst_yyyymmdd:
        all_yyyy.append(yyyymmdd[:4])
        all_mm.append(yyyymmdd[4:6])
        all_dd.append(yyyymmdd[-2:])
    # remove duplicates and sort
    all_yyyy = sorted(list(set(all_yyyy)))
    all_mm = sorted(list(set(all_mm)))
    all_dd = sorted(list(set(all_dd)))
    # generate regex string for time
    re_yyyy = "{" + ",".join(all_yyyy) + "}"
    re_mm = "{" + ",".join(all_mm) + "}"
    re_dd = "{" + ",".join(all_dd) + "}"

    partition = get_partition_cdc(re_yyyy, re_mm, re_dd)
    if data_format is None:
        source_uri = f"{dimentions_table}/{topic}/{partition}"
    else:
        source_uri = f"{dimentions_table}/{topic}/{partition}/*.{data_format}"
    # gc_bucket prefix
    if gc_bucket:
        uri_gc_bucket = f"gs://{gc_bucket}"
        source_uri = f"{uri_gc_bucket}/{source_uri}"
    return source_uri


def get_partition_cdc(yyyy, mm, dd):
    """
    return str

    :type yyyy: str
    :type mm: str
    :type dd: str
    """
    return f"year={yyyy}/month={mm}/day={dd}"


def get_partition_sqls_cdc(lst_yyyymmdd):
    """
    return list of partition condition query for sql

    :type lst_yyyymmdd: lst of str
    """
    partition_sqls = []
    for yyyymmdd in lst_yyyymmdd:
        sql_cond = (
            f"year={yyyymmdd[:4]} and month={yyyymmdd[4:6]} and day={yyyymmdd[6:]}"
        )
        partition_sqls.append(sql_cond)
    return partition_sqls


def get_partition_fields_cdc():
    """
    return dict
    """
    cdc_partitions = [
        {
            "type": "STRING",
            "field": "year",
        },
        {
            "type": "STRING",
            "field": "month",
        },
        {
            "type": "STRING",
            "field": "day",
        },
    ]
    return cdc_partitions


def get_list_prefix(
        db_source,
        table_name,
        is_fact=False,
        server_keys=None,
        from_date=None,
        to_date=None,
):
    """
    Get list prefix in GCS
    """
    ls_prefix = []
    if server_keys is None:
        server_keys = [0]
    for server in server_keys:
        if is_fact:
            for extract_date in pd.date_range(start=from_date, end=to_date).tolist():
                extract_date = extract_date.strftime("%Y-%m-%d")
                file_uri = f"facts_table/{db_source}/{extract_date}/shard_{server}/{table_name}"
                ls_prefix.append(file_uri)
        else:
            file_uri = (
                f"dimensions_table/{db_source}/{from_date}/shard_{server}/{table_name}"
            )
            ls_prefix.append(file_uri)

    return ls_prefix


def get_hdfs_path(
        mys: BaseKvConnectUtil,
        layer: str,
        bucket: str,
        business_day: str = "19700101",
        is_end_with_slash: bool = True,
) -> str:
    if mys is None:
        return ""

    # build hdfs_path
    if layer == RAW:
        hdfs_path = f"hdfs://{mys.host}:{mys.port}/{bucket}/{layer}/{mys.table_name_raw}/{business_day}/"
    else:
        hdfs_path = f"hdfs://{mys.host}:{mys.port}/{bucket}/{layer}/{mys.table_name}/"

    # remove end slash if specify not is_end_with_slash
    if not is_end_with_slash:
        hdfs_path = hdfs_path[:-1]

    return hdfs_path


def get_hdfs_path_v2(
        bucket: str,
        layer: str,
        table_name: str,
        business_day: str = None,
        is_end_with_slash: bool = True,
) -> str:
    """
    build hdfs path without requiring host,port
    """
    # build hdfs_path
    if business_day:
        hdfs_path = f"/{bucket}/{layer}/{table_name}/{business_day}/"
    else:
        hdfs_path = f"/{bucket}/{layer}/{table_name}/"

    # remove end slash if specify not is_end_with_slash
    if not is_end_with_slash:
        hdfs_path = hdfs_path[:-1]

    return hdfs_path


def get_hdfs_base_path(hdfs_path: str, layer: str) -> str:
    """
    hdfs_path: getting from func: get_hdfs_path
    """
    # if layer == RAW then need to remove business_day in path
    # else keep the original path

    if layer == RAW:
        hdfs_path_no_end_slash = hdfs_path[:-1] if hdfs_path[-1] == "/" else hdfs_path
        base_path = "/".join(hdfs_path_no_end_slash.split("/")[:-1])
    else:
        base_path = hdfs_path

    return base_path


def get_raw_delete_time_expr(int_year, int_month, int_day=None):
    """
    return
    (
       all_prev_years_expr (str) which is 4 years starting from int_year-1, eg. "{2022,2021,2020,2019}"
       , all_prev_months_expr (str) which is all months starting from int_month, eg. "{02,01}"
       , all_prev_days_expr (str): all day until int_day of that month/year
    )

    :type int_year: int
    :type int_month: int
    :type int_day: int
    """
    # get 4 latest years starting from the previous year
    all_prev_years = [str(int_year - y) for y in range(1, 5)]
    # years_expr like {2022,2023}
    all_prev_years_expr = "{" + ",".join(all_prev_years) + "}"

    # get all months from January to 'int_month' for making path on current considered year
    all_prev_months = [str(int_month - m) for m in range(int_month)]
    # adding "0" to single number month (1 -> 9) to make 01,02,..
    all_prev_months = [
        f"0{month}" if len(month) == 1 else month for month in all_prev_months
    ]
    # months_expr like {01,11}
    all_prev_months_expr = "{" + ",".join(all_prev_months) + "}"

    if int_day is not None:
        all_prev_days = [str(int_day - m) for m in range(1, int_day)]
        all_prev_days = [
            f"0{day}" if len(day) == 1 else day for day in all_prev_days
        ]
        all_prev_days_expr = "{" + ",".join(all_prev_days) + "}"
    else:
        all_prev_days_expr = None
    return all_prev_years_expr, all_prev_months_expr, all_prev_days_expr


def get_hdfs_path_to_clean(bucket: str,
                           layer: str,
                           table_name: str,
                           partition,
                           date: List[str]
                           ) -> List[str]:
    list_path = []
    for d in date:
        if partition:
            path = f"{bucket}/{layer}/{table_name}/{partition}/{d}"
        else:
            path = f"{bucket}/{layer}/{table_name}/{d}"
        list_path.append(path)
    return list_path
