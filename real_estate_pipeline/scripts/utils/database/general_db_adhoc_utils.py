from airflow.example_dags.utils.database.db_data_type import UpsertType
import re

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
