import array
import os
import sys
import pandas as pd
from typing import List

abs_path = os.path.dirname(os.path.abspath(__file__)) + "/../../.."
sys.path.append(abs_path)

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

def get_hdfs_path(
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
