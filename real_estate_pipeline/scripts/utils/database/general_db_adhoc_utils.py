from airflow.example_dags.utils.database.db_data_type import UpsertType
import re


class SourceDB:
    GOOGLE_BIGQUERY = "GB"
    POSTGRES = "PG"
    MYSQL = "MYSQL"
    MSSQL = "MSSQL"


class LogicRelation:
    AND = "and"
    OR = "or"
    EMPTY = ""


class GroupChar:
    OPEN = "("
    CLOSE = ")"
    EMPTY = ""


class DatabaseMappingType:
    BQ_2_POSTGRES_TYPE = {
        "INTEGER": "INT8",
        "NUMERIC": "DECIMAL",
        "FLOAT64": "FLOAT8",
        "FLOAT": "FLOAT8",
        "STRING": "TEXT",
        "BOOLEAN": "BOOL",
        "TIMESTAMP": "TIMESTAMPTZ",
        "DATETIME": "TIMESTAMP",
        "DATE": "DATE",
        "TIME": "TIME",
    }

    BQ_2_POSTGRES_PARTITION_TYPE = {"TIMESTAMP": UpsertType.DAY}

    BQ_2_UPSERT_TYPE = {
        "TIMESTAMP": UpsertType.DAY,
        "DATETIME": UpsertType.DAY,
        "DATE": UpsertType.DAY,
        "TIME": UpsertType.DAY,
        "INTEGER": UpsertType.INTEGER,
        "NUMERIC": UpsertType.NUMERIC,
        "STRING": UpsertType.STRING,
        "BOOLEAN": UpsertType.BOOLEAN,
    }

    BQ_2_SPARK_SQL_TYPE = {
        "NUMERIC": "DECIMAL(38,9)",
        "INTEGER": "BIGINT",
        "INT64": "BIGINT",
        "INT": "BIGINT",
        "TIMESTAMP": "TIMESTAMP",
        "DATETIME": "TIMESTAMP",
        "DATE": "DATE",
        "FLOAT": "DOUBLE",
        "FLOAT64": "DOUBLE",
        "BOOLEAN": "BOOLEAN",
        "STRING": "STRING",
        "BYTES": "BINARY"
    }


class DatabaseMappingOption:
    BQ_2_POSTGRES_MODE = {"REQUIRED": "NOT NULL", "NULLABLE": ""}


class DatabaseKeyword:
    POSTGRES_KEYWORD = dict.fromkeys(
        [
            "ADD",
            "ALL",
            "ALTER",
            "AND",
            "ANY",
            "AS",
            "ASC",
            "BETWEEN",
            "BY",
            "CASE",
            "CHECK",
            "COLUMN",
            "CONSTRAINT",
            "CREATE",
            "DATABASE",
            "DEFAULT",
            "DELETE",
            "DESC",
            "DISTINCT",
            "DROP",
            "EXEC",
            "EXISTS",
            "FOREIGN",
            "FROM",
            "FULL",
            "GROUP",
            "HAVING",
            "IN",
            "INDEX",
            "INNER",
            "INSERT",
            "INTO",
            "IS",
            "JOIN",
            "KEY",
            "LEFT",
            "LIKE",
            "LIMIT",
            "NOT",
            "NULL",
            "OR",
            "ORDER",
            "OUTER",
            "PRIMARY",
            "PROCEDURE",
            "REPLACE",
            "RIGHT",
            "SELECT",
            "SET",
            "TABLE",
            "TRUNCATE",
            "UNION",
            "UNIQUE",
            "UPDATE",
            "VALUES",
            "VIEW",
            "WHERE",
        ],
        1,
    )


class RenderTemplate:
    @staticmethod
    def get_sql_queries(sql, query_params):
        """
        :param sql: path to file .sql or a list string of query sql
                    ex. "dags/sql/dwh/kv_mssql/merge_warehouse.sql"
        :type sql: str or list[str]
        :type query_params: dict
        """
        # sql_script can contain multiple sql queries splited by ';\n'
        if isinstance(sql, str):
            with open(sql) as fi:
                lst_sql_script = [fi.read()]
        else:
            lst_sql_script = sql

        # replace params in sql_scripts with values
        sql_queries = []
        for sql_script in lst_sql_script:
            for pk, pv in query_params.items():
                pk = "{{params.%s}}" % pk
                sql_script = sql_script.replace(pk, pv)
            sql_queries.append(sql_script)
        sql_queries = [sql_query for sql_query in sql_queries if sql_query.strip()]

        return sql_queries
