from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin
from airflow.example_dags.utils.spark_thrift.connections import get_spark_thrift_conn
from airflow.example_dags.utils.database.spark_sql_adhoc_utils import SparkSqlAdhoc
from airflow.example_dags.utils.iceberg import iceberg_properties_utils
from airflow.example_dags.utils.iceberg.iceberg_config_utils import num_retention_snapshot
import re


class IcebergOperator(BaseOperator):
    template_fields = ["sql", "is_adhoc_create_target_table", "is_clean_iceberg_table", "iceberg_db",
                       "iceberg_table_name", "iceberg_table_uri", "num_keep_retention_snaps"]
    template_ext = (".sql",)
    ui_color = "#e4f0e8"

    @apply_defaults
    def __init__(
            self,
            hive_server2_conn_id,
            sql="",
            params=dict(),
            is_adhoc_create_target_table=False,
            iceberg_table_uri=None,
            iceberg_table_schema=None,
            iceberg_table_props=None,
            str_timetz_expire_snaps=None,
            num_keep_retention_snaps=num_retention_snapshot,
            iceberg_db="default",
            iceberg_table_name=None,
            is_clean_iceberg_table=False,
            *args,
            **kwargs,
    ):
        super(IcebergOperator, self).__init__(*args, **kwargs)
        self.hive_server2_conn_id = hive_server2_conn_id
        self.sql = sql[len("dags/"):] if sql.find("dags/") != -1 else sql
        self.params = params
        self.is_adhoc_create_target_table = is_adhoc_create_target_table
        self.iceberg_table_uri = iceberg_table_uri
        self.iceberg_table_schema = iceberg_table_schema
        self.iceberg_table_props = iceberg_table_props
        self.str_timetz_expire_snaps = (
            str_timetz_expire_snaps
            if str_timetz_expire_snaps is not None
            else SparkSqlAdhoc.get_str_of_timetz(days=1)
        )
        self.num_keep_retention_snaps = num_keep_retention_snaps
        self.iceberg_db = iceberg_db
        self.iceberg_table_name = iceberg_table_name
        self.is_clean_iceberg_table = is_clean_iceberg_table

    def get_iceberg_conn(self):
        conn = get_spark_thrift_conn(self.hive_server2_conn_id)
        return conn

    def validate_adhoc_create_target_table(self):
        if self.is_adhoc_create_target_table:
            # raise error if missing table info for adhoc creating target table
            if not (self.iceberg_table_uri and self.iceberg_table_schema):
                error_msgs = []
                if not self.iceberg_table_uri:
                    error_msgs.append(
                        f"ERROR: is_adhoc_create_target_table=True, need define iceberg_table_uri (current: {self.iceberg_table_uri})"
                    )
                if not self.iceberg_table_schema:
                    error_msgs.append(
                        f"ERROR: is_adhoc_create_target_table=True, need define iceberg_table_schema (current: {self.iceberg_table_schema})"
                    )
                error_message = "\n\n" + "\n".join(error_msgs) + "\n\n"
                raise Exception(error_message)

            # show guidelines for adhoc create target table
            info_guild_msgs = []
            if self.iceberg_db == "default":
                info_guild_msgs.append(
                    "INFO: you can define iceberg_db rather than 'default'"
                )
            if not self.iceberg_table_props:
                info_guild_msgs.append(
                    "INFO: you can define iceberg_table_props rather than using default settings"
                )
            if info_guild_msgs:
                info_guild_message = "\n\n" + "\n".join(info_guild_msgs) + "\n\n"
                print(info_guild_message)

    def call_setup_iceberg_table_props(self):
        # if not exists then init
        if self.iceberg_table_props is None:
            self.iceberg_table_props = dict()

        # setup iceberg table properties
        general_props = iceberg_properties_utils.get_general_iceberg_table_props()
        for k, v in general_props.items():
            if k not in self.iceberg_table_props:
                self.iceberg_table_props[k] = v

    def call_expire_snapshots(self, cursor):
        """
        expire snapshots from target iceberg table

        :param cursor: database connection cursor
        type cursor: obj
        """
        snapshot_del_sql = f"""
        CALL iceberg.system.expire_snapshots (
            table => '{self.iceberg_db}.{self.iceberg_table_name}', 
            older_than => TIMESTAMP '{self.str_timetz_expire_snaps}', 
            retain_last => {self.num_keep_retention_snaps}
        )
        """
        print(
            f"\nKeep {self.num_keep_retention_snaps} latest snapshots\n"
        )
        cursor.execute(snapshot_del_sql)

    def call_remove_orphan_files(self, cursor):
        """
        remove orphan files (metadata and data files not used) from target iceberg table

        :param cursor: database connection cursor
        type cursor: obj
        """
        timetz_str_remove_files = SparkSqlAdhoc.get_str_of_timetz(days=-2)
        orphan_files_del_sql = f"""
        CALL iceberg.system.remove_orphan_files (
            table => '{self.iceberg_db}.{self.iceberg_table_name}', 
            older_than => TIMESTAMP '{timetz_str_remove_files}'
        )
        """
        print(
            f"\nRemove orphan files older than {timetz_str_remove_files} \n"
        )
        cursor.execute(orphan_files_del_sql)

    def call_rewrite_manifests(self, cursor):
        """
        remove orphan files (metadata and data files not used) from target iceberg table

        :param cursor: database connection cursor
        type cursor: obj
        """
        rewrite_manifests_sql = f"""
        CALL iceberg.system.rewrite_manifests('{self.iceberg_db}.{self.iceberg_table_name}')
        """
        cursor.execute(rewrite_manifests_sql)

    def create_target_iceberg_table(self, cursor):
        # init iceberg table properties
        self.call_setup_iceberg_table_props()

        # create database if not exists
        db_create_sql = f"create database if not exists {self.iceberg_db};"
        # print(db_create_sql)
        cursor.execute(db_create_sql)

        # create output iceberg table
        tbl_sqls = SparkSqlAdhoc.gen_create_tbl_sqls(
            table_name=self.iceberg_table_schema.TABLE_NAME,
            table_columns_schema=self.iceberg_table_schema.COLUMNS_SCHEMA,
            db_name=self.iceberg_db,
            location=self.iceberg_table_uri,
            partitions=self.iceberg_table_schema.TIME_PARTITIONING,
            data_format="iceberg",
            table_properties=self.iceberg_table_props,
            normalize_to_str=False,
        )
        # print(tbl_sqls[0])
        cursor.execute(f"{tbl_sqls[0]}")

    def run_sql_script(self, cursor):
        # sql_script can contain multiple sql queries splited by ';\n'
        sql_script = self.sql

        # get sql_queries from sql_script, just get un-empty string
        # strip sql_query
        sql_queries = [
            sql_query.strip()
            for sql_query in re.split(r";\s*\n", sql_script)
            if sql_query.strip()
        ]
        # remove every ';' at the end of sql_query
        sql_queries = [
            sql_query[:-1] if sql_query[-1] == ";" else sql_query
            for sql_query in sql_queries
        ]
        # remove comment like "-- select 1 as t;"
        sql_queries = [
            sql_query
            for sql_query in sql_queries
            if
            # ignore commented sql_query like "-- select 1 as t;"
            # do not ignore "-- select 1 as t \n select 2 as a"
            not (sql_query.strip()[:2] == "--" and sql_query.strip().find("\n") == -1)
        ]

        for sql_query in sql_queries:
            sql_log_info = """\n\nRun:
            *****
            %s
            """
            # print(sql_log_info % sql_query)
            cursor.execute(sql_query)

    def execute(self, context):
        # init iceberg connection
        conn = self.get_iceberg_conn()
        cursor = conn.cursor()

        # Create target Iceberg table if specified
        if self.is_adhoc_create_target_table:
            self.validate_adhoc_create_target_table()
            self.create_target_iceberg_table(cursor)

        # Execute self.sql from input
        self.run_sql_script(cursor)

        # remove unused snapshots and metadata files
        '''if self.is_clean_iceberg_table:
            # assert conditions meet for is_clean_iceberg_table
            assert self.iceberg_table_name, \
                "iceberg_table_name param need to be specified for is_clean_iceberg_table=True"
            assert self.iceberg_db, \
                "iceberg_db need to be specified for is_clean_iceberg_table=True"
            # do clean iceberg table
            self.call_expire_snapshots(cursor)
            self.call_remove_orphan_files(cursor)
            self.call_rewrite_manifests(cursor)'''
        if self.is_clean_iceberg_table:
            self.call_expire_snapshots(cursor)
            self.call_remove_orphan_files(cursor)
            self.call_rewrite_manifests(cursor)

        # release iceberg connection
        cursor.close()
        conn.close()


class IcebergOperatorPlugin(AirflowPlugin):
    name = "iceberg_operator"
    operators = [IcebergOperator]
