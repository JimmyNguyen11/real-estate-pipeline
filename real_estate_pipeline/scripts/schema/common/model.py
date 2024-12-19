FACT_TABLE_TYPE = "FACT"
DIM_TABLE_TYPE = "DIM"


class DwhLoadType:
    TRANSFORMED_STAGING = "TRANSFORMED_STAGING"
    STAGING = "STAGING"  # default

class BaseModel:
    def __init__(self, table_name):
        self.TABLE_NAME = table_name
        self.DEFAULT_COLUMNS = [
            {"name": "ETL_DATE", "mode": "NULLABLE", "type": "TIMESTAMP"},
            {"name": "BUSINESS_DATE", "mode": "NULLABLE", "type": "STRING"},
        ]
        self.COLUMNS = []
        self.COLUMNS_SCHEMA = []
        self.TIME_PARTITIONING = None
        self.CLUSTERING = None
        self.TABLE_TYPE = DIM_TABLE_TYPE
        self.SERVER_NAME = ""  # Kafka connect
        self.DB_NAME = ""  # Kafka connect
        self.KEY_COLUMNS = []  # Kafka connect
        self.AUTO_GEN_SQL = True  # Kafka connect
        self.IS_AGGREGATED = False  # Kafka connect
        self.DATA_QUALITY = []  # Use for Data Quality config
        self.DEFAULT_TABLE_NAME = ""  # Tên bảng ở db nguồn, sử dụng trong migration

    def get_table_name(self):
        return self.TABLE_NAME

    def get_list_columns(self, columns, wrap_char="`"):
        wrap_char = "" if wrap_char is None else wrap_char
        list_columns = []
        for c in columns:
            # if column_name does not have special char (`) or use alias then use wrap_char
            if c["name"][0] != "`" and c["name"].find(" ") == -1:
                col = f"{wrap_char}{c['name']}{wrap_char}"
            else:
                col = f"{c['name']}"
            list_columns.append(col)
        return list_columns

    def get_list_name_type_columns(self, columns):
        list_columns = []
        all_column_schemas = columns
        for c in all_column_schemas:
            col_name = f'{c["name"]}'
            col_type = f'{c["type"]}'
            col = f'{col_name} {col_type}'
            list_columns.append(col)
        all_field = ',\n'.join(map(str, list_columns))
        return all_field

class DimDate(BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.COLUMNS = [
            {"name": "date", "mode": "NULLABLE", "type": "DATE"},
            {"name": "day_of_week", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "day_of_month", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "week", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "month", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "quarter", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "quarter_name", "mode": "NULLABLE", "type": "STRING"},
            {"name": "year", "mode": "NULLABLE", "type": "INTEGER"},
            {"name": "month_full_name", "mode": "NULLABLE", "type": "STRING"},
            {"name": "week_day", "mode": "NULLABLE", "type": "STRING"},
        ]
        self.COLUMNS_SCHEMA = self.COLUMNS
        self.TABLE_TYPE = DIM_TABLE_TYPE

    def get_list_columns(self, columns):
        return super().get_list_columns(columns)
