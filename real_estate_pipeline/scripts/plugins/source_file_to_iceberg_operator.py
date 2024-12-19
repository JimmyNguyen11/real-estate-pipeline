"""
This module contains a Google BigQuery to Postgres operator.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.example_dags.utils.spark_thrift.connections import get_spark_thrift_conn
from airflow.example_dags.utils.database.spark_sql_adhoc_utils import SparkSqlAdhoc
from airflow.example_dags.utils.database import schemas_utils
from airflow.example_dags.utils.iceberg import iceberg_properties_utils
from airflow.example_dags.utils.iceberg.iceberg_config_utils import (
    max_partition_num_line,
    num_retention_snapshot,
)
import math


class SourceFileToIcebergOperator(BaseOperator):
    template_fields = (
        "source_file_uri",
        "source_file_format",
        "source_file_options",
        "iceberg_db",
        "iceberg_table_name",
        "iceberg_table_uri",
        "hive_num_partition",
        "is_dedup_source",
        "replace_iceberg_table",
        "iceberg_write_truncate",
    )
    ui_color = "#e4f0e8"

    @apply_defaults
    def __init__(
            self,
            source_file_uri,
            iceberg_table_uri,
            iceberg_table_schema,
            hive_server2_conn_id,
            source_file_format="parquet",
            source_file_options=None,
            iceberg_db="default",
            str_timetz_expire_snaps=None,
            num_keep_retention_snaps=num_retention_snapshot,
            replace_iceberg_table=False,
            iceberg_table_props=None,
            iceberg_write_truncate=False,
            hive_num_partition=None,
            is_dedup_source=True,
            is_alter=False,
            *args,
            **kwargs,
    ):
        super(SourceFileToIcebergOperator, self).__init__(*args, **kwargs)
        self.source_file_uri = source_file_uri
        # clean uri by removing / at the end of target_table_uri if exists
        self.iceberg_table_uri = (
            iceberg_table_uri
            if iceberg_table_uri[-1] != "/"
            else iceberg_table_uri[:-1]
        )
        self.iceberg_table_schema = iceberg_table_schema
        self.hive_server2_conn_id = hive_server2_conn_id
        self.source_file_format = source_file_format
        self.source_file_options = source_file_options
        self.iceberg_db = iceberg_db
        self.str_timetz_expire_snaps = (
            str_timetz_expire_snaps
            if str_timetz_expire_snaps is not None
            else SparkSqlAdhoc.get_str_of_timetz(days=1)
        )
        self.num_keep_retention_snapshot = num_keep_retention_snaps
        self.iceberg_table_props = iceberg_table_props
        self.replace_iceberg_table = replace_iceberg_table
        self.iceberg_write_truncate = iceberg_write_truncate
        self.hive_num_partition = hive_num_partition
        self.is_dedup_source = is_dedup_source
        self.is_alter = is_alter

        self.iceberg_table_name = self.iceberg_table_schema.TABLE_NAME

        self.is_source_parquet = self.source_file_format.find("parquet") != -1
        if hasattr(self.iceberg_table_schema, "COLUMNS_SCHEMA") and self.iceberg_table_schema.COLUMNS_SCHEMA:
            self.table_columns_schema = self.iceberg_table_schema.COLUMNS_SCHEMA
        elif self.is_source_parquet:
            self.table_columns_schema = None
        else:
            raise Exception("table_columns_schema None or Empty List only support for PARQUET source file")

    def get_iceberg_conn(self):
        conn = get_spark_thrift_conn(self.hive_server2_conn_id)
        return conn

    def call_setup_iceberg_table_props(self):
        # if not exists then init
        if self.iceberg_table_props is None:
            self.iceberg_table_props = dict()

        # setup iceberg table properties
        general_props = iceberg_properties_utils.get_general_iceberg_table_props()
        for k, v in general_props.items():
            if k not in self.iceberg_table_props:
                self.iceberg_table_props[k] = v

    def create_external_tmp_dbdata_tbl(self, cursor, tmp_dbdata_tbl_name):
        create_table_with_schema = True
        if self.is_source_parquet:
            external_tmp_dbdata_tbl_sqls = SparkSqlAdhoc.gen_create_tbl_sqls(
                table_name=tmp_dbdata_tbl_name,
                table_columns_schema=self.table_columns_schema,
                location=self.source_file_uri,
                data_format="parquet",
                table_field_expr="",
            )
            try:
                cursor.execute(f"{external_tmp_dbdata_tbl_sqls[0]}")
                create_table_with_schema = False
            except Exception as ex:
                print("WARN: Source file PARQUET having no records ---> Create empty tmp table")

        if create_table_with_schema:
            # ex. CREATE TABLE if not exists default.table_name (id bigint)
            # USING data_format
            # LOCATION source_file_uri;
            external_tmp_dbdata_tbl_sqls = SparkSqlAdhoc.gen_create_tbl_sqls(
                table_name=tmp_dbdata_tbl_name,
                table_columns_schema=self.table_columns_schema,
                location=self.source_file_uri,
                data_format=self.source_file_format,
                data_options=self.source_file_options,
            )
        # execute sql
        cursor.execute(f"{external_tmp_dbdata_tbl_sqls[0]}")

    def call_expire_snapshots(self, cursor):
        snapshot_del_sql = f"""
        CALL iceberg.system.expire_snapshots (
            table => '{self.iceberg_db}.{self.iceberg_table_name}', 
            older_than => TIMESTAMP '{self.str_timetz_expire_snaps}', 
            retain_last => {self.num_keep_retention_snapshot}
        )
        """
        print(
            f"Keep {self.num_keep_retention_snapshot} latest snapshots"
        )
        cursor.execute(snapshot_del_sql)

    def call_remove_orphan_files(self, cursor):
        timetz_str_remove_files = SparkSqlAdhoc.get_str_of_timetz(days=-2)
        orphan_files_del_sql = f"""
        CALL iceberg.system.remove_orphan_files (
            table => '{self.iceberg_db}.{self.iceberg_table_name}', 
            older_than => TIMESTAMP '{timetz_str_remove_files}'
        )
        """
        print(
            f"Remove orphan files older than {timetz_str_remove_files}",
        )
        cursor.execute(orphan_files_del_sql)

    def call_rewrite_manifests(self, cursor):
        rewrite_manifests_sql = f"""
        CALL iceberg.system.rewrite_manifests('{self.iceberg_db}.{self.iceberg_table_name}')
        """
        cursor.execute(rewrite_manifests_sql)

    def get_hive_table_columns_schema(self, cursor, hive_table_name):
        cursor.execute(f"desc {hive_table_name}")
        columns_schema = cursor.fetchall()
        table_columns_schema = []
        for row in columns_schema:
            tmp = dict()
            tmp["name"] = row[0]
            tmp["type"] = row[1]
            table_columns_schema.append(tmp)
        return table_columns_schema
        
    def get_distinct_expr(self):
        if self.is_dedup_source:
            distinct_expr = "distinct"
        else:
            distinct_expr = ""
        return distinct_expr

    def execute(self, context):
        # init iceberg connection
        conn = self.get_iceberg_conn()
        cursor = conn.cursor()

        # init iceberg table properties
        self.call_setup_iceberg_table_props()

        # CREATE TARGET DATABASE

        db_create_sql = f"create database if not exists {self.iceberg_db};"
        # print(db_create_sql)
        cursor.execute(db_create_sql)

        # CREATE TABLE tmp_dbdata_tbl_name

        # tmp_dbdata_tbl_name is external table name
        # this stores source data at specific date
        ori_dbname = self.iceberg_db.split(".")[-1]
        tmp_dbdata_tbl_name = (
            f"{ori_dbname}_{self.iceberg_table_name}_dbdata_tmp"
        )
        SparkSqlAdhoc.drop_table_if_exists(
            cursor=cursor, db_name="default", table_name=tmp_dbdata_tbl_name
        )
        self.create_external_tmp_dbdata_tbl(cursor, tmp_dbdata_tbl_name)

        # infer schema from parquet files
        if self.table_columns_schema is None and self.is_source_parquet:
            self.table_columns_schema = self.get_hive_table_columns_schema(
                cursor=cursor,
                hive_table_name=tmp_dbdata_tbl_name
            )

        # CREATE TARGET TABLE

        if not self.replace_iceberg_table:
            tbl_sqls = SparkSqlAdhoc.gen_create_tbl_sqls(
                table_name=self.iceberg_table_name,
                table_columns_schema=self.table_columns_schema,
                db_name=self.iceberg_db,
                location=self.iceberg_table_uri,
                partitions=self.iceberg_table_schema.TIME_PARTITIONING,
                data_format="iceberg",
                table_properties=self.iceberg_table_props,
                normalize_to_str=False,
            )
            # print(tbl_sqls[0])
            cursor.execute(f"{tbl_sqls[0]}")

        # UPDATE TABLE DATA

        # get iceberg table columns
        cols_for_select = self.iceberg_table_schema.get_list_columns(
            self.table_columns_schema
        )
        default_columns = []
        cols_for_select = default_columns + cols_for_select
        cols_expr_for_select = ",".join(cols_for_select)
        src_cols = [f"s.{col}" for col in cols_for_select]
        src_col_expr = ",".join(src_cols)
        src_tgt_update_cols = [f"t.{col}=s.{col}" for col in cols_for_select]
        src_tgt_update_col_expr = ",".join(src_tgt_update_cols)

        # write data to target iceberg table

        # detect num partition
        cursor.execute(
            f"""
            select count(1) from default.{tmp_dbdata_tbl_name};
            """
        )
        hive_num_line = float(cursor.fetchall()[0][0])
        hive_num_partition = (
            self.hive_num_partition
            if self.hive_num_partition
            else max(math.ceil(hive_num_line / max_partition_num_line), 1)
        )
        distinct_expr = self.get_distinct_expr()

        # execute sql to create target iceberg table
        if self.replace_iceberg_table:
            # create or replace target table (reduce downtime on table)
            select_stmt = f"""SELECT /*+ REPARTITION({hive_num_partition}) */ {distinct_expr} {cols_expr_for_select} 
                        FROM default.{tmp_dbdata_tbl_name}
                        """
            create_or_replace_sqls = SparkSqlAdhoc.gen_create_or_replace_iceberg_sqls(
                table_name=self.iceberg_table_name,
                db_name=self.iceberg_db,
                select_stmt=select_stmt,
                location=self.iceberg_table_uri,
                partitions=self.iceberg_table_schema.TIME_PARTITIONING,
                sort_column_expr=self.iceberg_table_schema.sort_column_expr,
                table_columns_schema=self.table_columns_schema,
                table_properties=self.iceberg_table_props,
            )
            cursor.execute(create_or_replace_sqls[0])
        elif self.iceberg_write_truncate:
            # insert overwrite data to target table
            insert_overwrite_sql = f"""INSERT OVERWRITE {self.iceberg_db}.{self.iceberg_table_name} ({cols_expr_for_select})
            SELECT /*+ REPARTITION({hive_num_partition}) */ {distinct_expr} {cols_expr_for_select} 
            FROM default.{tmp_dbdata_tbl_name};
            """
            # print(insert_overwrite_sql)
            cursor.execute(insert_overwrite_sql)
        else:
            if not self.iceberg_table_schema.KEY_COLUMNS:
                raise Exception("MERGE INTO require TABLE having KEY_COLUMNS attribute")

            # get iceberg table key columns
            key_fields = schemas_utils.get_field_names(
                self.iceberg_table_schema.KEY_COLUMNS, quote="`"
            )
            src_tgt_keys = [f"t.{k}=s.{k}" for k in key_fields]
            src_tgt_key_expr = " and ".join(src_tgt_keys)

            # merge to get latest record
            merge_sql = f"""MERGE INTO {self.iceberg_db}.{self.iceberg_table_name} t
            USING (SELECT /*+ REPARTITION({hive_num_partition}) */ {distinct_expr} {cols_expr_for_select} FROM default.{tmp_dbdata_tbl_name} ) s         
            ON {src_tgt_key_expr}          
            WHEN MATCHED THEN UPDATE SET {src_tgt_update_col_expr}
            WHEN NOT MATCHED THEN INSERT ({cols_expr_for_select}) VALUES ({src_col_expr});
            """
            # print(merge_sql)
            cursor.execute(merge_sql)

        # drop all tmp tables
        SparkSqlAdhoc.drop_table_if_exists(
            cursor=cursor, db_name="default", table_name=tmp_dbdata_tbl_name
        )
        
        if self.is_alter:
            first_sql = "CREATE TABLE iceberg.bds4_staging.demo1_tmp (`du_an` STRING, `price_VND` DOUBLE, `location` STRING, `status` STRING, `area` DOUBLE, `price_m2` DOUBLE, `toilet` BIGINT, `room` BIGINT, `doc` STRING, `type` STRING, `So_tang` BIGINT, `furnishing` STRING, `created_date` TIMESTAMP) USING iceberg LOCATION 'hdfs:///demo4/staging/demo1_tmp';"
            cursor.execute(first_sql)
            
            second_sql = "INSERT INTO iceberg.bds4_staging.demo1_tmp SELECT du_an, price_VND, location, status, area, price_m2, toilet, room, doc, type, So_tang, furnishing, CAST(created_date AS TIMESTAMP) AS created_date FROM iceberg.bds4_staging.demo1;"
            cursor.execute(second_sql)
            
            third_sql = "DROP TABLE iceberg.bds4_staging.demo1;"
            cursor.execute(third_sql)
            
            fourth_sql = "ALTER TABLE iceberg.bds4_staging.demo1_tmp RENAME TO iceberg.bds4_staging.demo1;"
            cursor.execute(fourth_sql)

        # release iceberg connection
        cursor.close()
        conn.close()


class SourceFileToIcebergOperatorPlugin(AirflowPlugin):
    name = "source_file_to_iceberg_operator"
    operators = [SourceFileToIcebergOperator]
