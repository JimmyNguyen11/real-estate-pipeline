from airflow.example_dags.utils.database import schemas_utils
from airflow.example_dags.utils.database.db_data_type import UpsertType
from airflow.example_dags.utils.date_time.date_time_utils import (
    get_business_date,
    get_timestamp_tz_from_datestr,
    float_to_datetime,
    date_2_str,
)
from typing import Dict
import re


class SparkSqlAdhoc:
    @staticmethod
    def gen_partition(table_columns_dict: Dict, normalize_to_str: bool, partition_row: Dict) -> str:
        col_name = partition_row["field"]

        # adding col_type if col_name not in table_columns_dict
        # meaning create that col in partition_expr for HIVE table (not using for iceberg)
        # else col_type is empty
        col_type = ""
        if col_name not in table_columns_dict:
            if normalize_to_str:
                col_type = "STRING"
            else:
                # col_type = mapper_2_spark_sql_type[partition_row["type"]]
                col_type = partition_row["type"]

        # add field to partition_fields
        if partition_row["type"] == UpsertType.DAY:
            field_expr = f"days({col_name}) {col_type}"
        else:
            field_expr = f"{col_name} {col_type}"

        return field_expr

    @staticmethod
    def gen_partition_expr(partitions, table_columns_schema, normalize_to_str: bool) -> str:
        # set partition for table
        partition_expr = ""
        if partitions is None:
            partition_expr = ""
        elif isinstance(partitions, str):
            partition_expr = partitions
        elif isinstance(partitions, list) or isinstance(partitions, dict):
            if table_columns_schema is None:
                raise Exception(f"partitions type {type(partitions)} requires table_columns_schema not None")
            table_columns = schemas_utils.get_field_names(table_columns_schema)
            table_columns_dict = dict.fromkeys(table_columns, 1)
            if isinstance(partitions, list):
                partition_fields = []
                for row in partitions:
                    field_expr = SparkSqlAdhoc.gen_partition(table_columns_dict, normalize_to_str, row)
                    partition_fields.append(field_expr)
                partition_expr = ",".join(partition_fields)
                partition_expr = f"PARTITIONED BY ({partition_expr})"
            elif isinstance(partitions, dict):
                partition_expr = SparkSqlAdhoc.gen_partition(
                    table_columns_dict, normalize_to_str, partitions
                )
                partition_expr = f"PARTITIONED BY ({partition_expr})"
        else:
            raise Exception("partitions must be list[dict] or dict or str")

        return partition_expr

    @staticmethod
    def gen_table_property_expr(table_properties: Dict) -> str:
        if table_properties:
            tbl_props = []
            for k, v in table_properties.items():
                prop = f"'{k}'='{v}'"
                tbl_props.append(prop)
            # comma separated props
            tbl_props_expr = ", ".join(tbl_props)
            # wrap by ()
            tbl_props_expr = f"TBLPROPERTIES ({tbl_props_expr})"
        else:
            tbl_props_expr = ""

        return tbl_props_expr

    @staticmethod
    def gen_create_tbl_sqls(
            table_name,
            table_columns_schema,
            db_name="default",
            location=None,
            partitions=None,
            data_format="json",
            data_options=None,
            table_field_expr=None,
            table_properties=None,
            normalize_to_str=False,
    ):
        # the sql which this func is building
        # Note:
        #  [OPTIONS, LOCATION, PARTITION, TBLPROPERTIES] will be appended then after
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS {db_name}.{table_name} {field_expr}
         USING {spark_format}
        """

        # mapper_2_spark_sql_type = DatabaseMappingType.BQ_2_SPARK_SQL_TYPE
        # sqls is the list sql returned
        sqls = []

        # spark_format is table format
        if data_format == "json":
            spark_format = "org.apache.spark.sql.json"
        elif data_format == "parquet":
            spark_format = "org.apache.spark.sql.parquet"
        else:
            spark_format = data_format

        # infer data_options
        if data_options:
            parsed_data_options = []
            for k, v in data_options.items():
                parsed_data_options += [f'"{k}"="{v}"']
            data_options_expr = ", ".join(parsed_data_options)
            create_table_sql += f" OPTIONS ({data_options_expr}) \n"

        # set partition for table
        partition_expr = SparkSqlAdhoc.gen_partition_expr(
            partitions=partitions,
            table_columns_schema=table_columns_schema,
            normalize_to_str=False,
        )
        create_table_sql += f" {partition_expr} \n"

        # generate schema for table (field_expr)
        # if table_field_expr is not specified value then generate field_expr from table_columns_schema
        if table_field_expr is None:
            if not table_columns_schema:
                raise Exception("table_field_expr None "
                                "require table_columns_schema is not None or Empty List")
            lst_field = []
            for colr in table_columns_schema:
                col_name = (
                    f'`{colr["name"]}`'
                    if schemas_utils.is_db_field(colr["name"])
                    else colr["name"]
                )
                # normalize_to_str means that all col type is string
                if normalize_to_str:
                    col_type = "STRING"
                else:
                    # col_type = mapper_2_spark_sql_type[colr["type"]]
                    col_type = colr["type"]
                field_expr = f"{col_name} {col_type}"
                lst_field.append(field_expr)
            # create fields_expr separated by \n
            field_expr = ",\n ".join(lst_field)
            # wrap by ()
            field_expr = f"({field_expr})"
        else:
            # set field_expr as empty, so the table schema will be got from data
            field_expr = table_field_expr

        # build tbl_properties
        tbl_props_expr = SparkSqlAdhoc.gen_table_property_expr(table_properties)
        create_table_sql += f" {tbl_props_expr} \n"

        # complete create_table_sql
        # print(db_name, table_name, field_expr, spark_format)
        # print(create_table_sql)
        create_table_sql = create_table_sql.format(
            db_name=db_name,
            table_name=table_name,
            field_expr=field_expr,
            spark_format=spark_format,
        )

        # if location is specified, creating external table else  creating managed table
        # put this after '.format' avoid keyword {} error
        if location:
            create_table_sql += f" LOCATION '{location}' \n"

        create_table_sql = re.sub(r"\n\s+\n", "\n", create_table_sql.strip())
        sqls.append(create_table_sql)
        return sqls

    @staticmethod
    def gen_create_or_replace_iceberg_sqls(
            table_name,
            db_name,
            select_stmt,
            location=None,
            partitions=None,
            sort_column_expr=None,
            table_columns_schema=None,
            table_properties=None,
    ):
        if partitions and not sort_column_expr:
            raise Exception("specify partitions require to specify sort_column_expr")

        # build tbl_properties
        tbl_props_expr = SparkSqlAdhoc.gen_table_property_expr(table_properties)

        # set partition for table
        partition_expr = SparkSqlAdhoc.gen_partition_expr(
            partitions=partitions,
            table_columns_schema=table_columns_schema,
            normalize_to_str=False,
        )

        sort_column_expr = "" if sort_column_expr is None else sort_column_expr

        create_table_sql = f"""
            CREATE OR REPLACE TABLE {db_name}.{table_name}
            USING iceberg
            {partition_expr}
            LOCATION '{location}'
            {tbl_props_expr}
            {select_stmt}
            {sort_column_expr}
            """
        create_table_sql = re.sub(r"\n\s+\n", "\n", create_table_sql.strip())
        sqls = [create_table_sql]
        return sqls

    @staticmethod
    def drop_table_if_exists(cursor, db_name, table_name, is_purge=False):
        purge_opt = "purge" if is_purge else ""
        drop_table_sql = f"""
                drop table if exists {db_name}.{table_name} {purge_opt};
                """
        cursor.execute(drop_table_sql)

    @staticmethod
    def get_str_of_timetz(days, tz=None):
        """
        return a string representing timestamp with tz

        param days: number of days add or remove from now()
        type days: int
        param tz: number of days add or remove from now()
        type tz: datetime.timezone
        """
        yyyymmdd = get_business_date(days=days)
        if tz:
            # use tz from func param
            timetz_int = get_timestamp_tz_from_datestr(yyyymmdd, hour=0, tz=tz)
        else:
            # use UTC timezone
            timetz_int = get_timestamp_tz_from_datestr(yyyymmdd, hour=0)
        timetz_dt = float_to_datetime(timetz_int)
        timetz_str = date_2_str(timetz_dt, "%Y-%m-%d %H:%M:%S.%f%z")
        return timetz_str
