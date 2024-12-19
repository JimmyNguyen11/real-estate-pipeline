# dict for mapping lakehouse to raw (pandas) data types
import pyarrow as pa

RAW_PANDAS_TYPE = "pandas"
RAW_PYARROW_TYPE = "pyarrow"

LAKEHOUSE_TO_PANDAS_TYPE = {
    "bigint": "int64",
    "long": "int64",
    "timestamp": "datetime64[ns]",
    "double": "float64",
    "boolean": "bool",
    "string": "string",
}

LAKEHOUSE_TO_PYARROW_TYPE = {
    "bigint": "int64",
    "long": "int64",
    "timestamp": "timestamp[ns]",
    "double": "float64",
    "boolean": "bool",
    "string": "string"
    #"integer": "int64"
}

PANDAS_TO_PYARROW_TYPE = {
    "int64": "int64",
    "datetime64[ns]": "timestamp[ns]",
    "float64": "float64",
    "bool": "bool",
    "string": "string",
    "Int32": "int32",
    "Int64": "int64"
}

LAKEHOUSE_TO_CLICKHOUSE_TYPE = {
    "int": "Int64",
    "bigint": "Int64",
    "string": "String",
    "date": "Date",
    "float": "Float64",
    "timestamp": "Timestamp",
    "double": "Float64",
    "boolean": "Boolean",
    "decimal(38, 6)": "Decimal(38, 6)",
    "decimal(38, 7)": "Decimal(38, 7)",
    "decimal(38, 8)": "Decimal(38, 8)",
    "decimal(38, 9)": "Decimal(38, 9)",
}

def get_type_pyarrow_from_pandas(pandas_schema):
    pyarrow_columns = dict()
    for col_name, pandas_type in pandas_schema.items():
        pandas_type = pandas_type.lower()
        if pandas_type == "str":
            raise Exception("pandas_type (str) not supported, please use (string)")
        pyarrow_type = PANDAS_TO_PYARROW_TYPE[pandas_type]
        pyarrow_columns[col_name] = pyarrow_type
    return pyarrow_columns


def get_raw_columns(lakehouse_columns, raw_type=RAW_PANDAS_TYPE):
    raw_columns = dict()

    # assign lakehouse_to_raw_type to chosen conversion type based on raw_type
    # lakehouse_to_raw_type should not modify assigned value because the assigned value is constant
    lakehouse_to_raw_type = LAKEHOUSE_TO_PANDAS_TYPE
    if raw_type == RAW_PYARROW_TYPE:
        lakehouse_to_raw_type = LAKEHOUSE_TO_PYARROW_TYPE

    for lakehouse_col in lakehouse_columns:
        lakehouse_type = lakehouse_col["type"].lower()
        col_raw_type = lakehouse_to_raw_type[lakehouse_type]
        raw_columns[lakehouse_col["name"]] = col_raw_type

    return raw_columns

def get_pyarrow_fields_schema(pyarrow_schema):
    pa_fields = []
    for col_name, data_type in pyarrow_schema.items():
        pa_fields.append(pa.field(col_name, data_type))
    pa_fields_schema = pa.schema(pa_fields)
    return pa_fields_schema


def remove_string_column(raw_columns):
    converted_columns = {}
    for col_name, col_type in raw_columns.items():
        if col_type not in ('str', 'string'):
            converted_columns[col_name] = col_type
    return converted_columns


def get_pyarrow_table(pandas_df, pyarrow_schema):
    pyarrow_fields_schema = get_pyarrow_fields_schema(pyarrow_schema)
    table = pa.Table.from_pandas(df=pandas_df, schema=pyarrow_fields_schema)
    return table

def get_staging_columns(warehouse_columns):
    staging_columns = []

    for warehouse_col in warehouse_columns:
        staging_col = dict()
        staging_col["name"] = warehouse_col["name"]
        staging_col["mode"] = warehouse_col["mode"]
        warehouse_type = warehouse_col["type"].lower()

        # if in rules of mapping then do mapping data types then use some rules mapping
        if is_in_warehouse_mapping_rules(warehouse_type=warehouse_type):
            # case-1: all types convert to "string" type in staging
            if warehouse_type.find("decimal") != -1 or warehouse_type == "date":
                staging_col["type"] = "string"
        # use the same type as warehouse_col
        else:
            staging_col["type"] = warehouse_col["type"]

        staging_columns.append(staging_col)

    return staging_columns
