from airflow.example_dags.utils.database.general_db_adhoc_utils import GroupChar


class WriteDisposition:
    # WRITE_TRUNCATE overwrites data
    # in Bigquery, if the table already exists, BigQuery overwrites the data,
    # removes the constraints, and uses the schema from the query result
    WRITE_TRUNCATE = "WRITE_TRUNCATE"
    # WRITE_APPEND appends data only
    # in Bigquery, if the table already exists, BigQuery appends the data to the table.
    WRITE_APPEND = "WRITE_APPEND"
    # WRITE_EMPTY write only when table is empy
    # in Bigquery, if the table already exists and contains data, a 'duplicate' error is returned in the job result.
    WRITE_EMPTY = "WRITE_EMPTY"
    # MERGE do merge operation
    WRITE_MERGE = "WRITE_MERGE"


class CreateDisposition:
    # the table must already exist
    CREATE_NEVER = "CREATE_NEVER"
    # If the table does not exist, creates the table
    CREATE_IF_NEEDED = "CREATE_IF_NEEDED"


def is_db_field(col_name):
    """
    Return true if column_name does not have special char (`) or contain ' '
    """
    if col_name[0] != '`' and col_name.find(' ') == -1:
        return True
    return False


def get_field_names(columns_schema, quote=''):
    """
    Return a list of field_names (list[str]) from table_schema
    :type columns_schema: list of dict
    :param columns_schema:  ex.COLUMNS_SCHEMA in class schema_postgres_datamart.py
    :type quote: str
    :param quote: quote char for wrapping fields
    """
    quote = '' if quote is None else quote
    list_columns = []
    for c in columns_schema:
        # if column_name does not have special char (`) or use alias then use quote
        if is_db_field(c['name']):
            col = f"{quote}{c['name']}{quote}"
        else:
            col = c['name']
        list_columns.append(col)
    return list_columns


def gen_upsert_cond_expr(upsert_fields, quote=''):
    """
    Return a str representing condition for UPSERT_FIELDS
    ex. return: a=%s and b=%s

    :type upsert_fields: list of dict
    :param upsert_fields: ex.UPSERT_FIELDS in class schema_postgres_datamart.py
    :type quote: str
    :param quote: quote char for wrapping fields
    """
    upsert_sql_cond = ""
    for key_field in upsert_fields:
        key_col = f"{quote}{key_field['field']}{quote}"
        u_logic_rel = key_field["logic_relation"]
        u_group_char = key_field["group_char"]

        logic_expr = u_logic_rel
        if u_group_char == GroupChar.OPEN:
            logic_expr = f"{u_logic_rel} {GroupChar.OPEN}"
        close_group = GroupChar.CLOSE if u_group_char == GroupChar.CLOSE else GroupChar.EMPTY

        upsert_sql_cond += f" {logic_expr} {key_col}=%s {close_group}"
    return upsert_sql_cond.strip()


def gen_extract_cond_expr(extract_fields, quote=''):
    upsert_sql_cond = ""
    for key_field in extract_fields:
        key_col = f"{quote}{key_field['field']}{quote}"
        u_logic_rel = key_field["logic_relation"]
        u_group_char = key_field["group_char"]

        logic_expr = u_logic_rel
        if u_group_char == GroupChar.OPEN:
            logic_expr = f"{u_logic_rel} {GroupChar.OPEN}"
        close_group = GroupChar.CLOSE if u_group_char == GroupChar.CLOSE else GroupChar.EMPTY

        upsert_sql_cond += f" {logic_expr} {key_col}=%s {close_group}"
    return upsert_sql_cond.strip()


def get_key_columns(upsert_fields, columns_schema, quote=''):
    if upsert_fields is None or columns_schema is None:
        return [], []
    key_fields = [f"{quote}{f['field']}{quote}" for f in upsert_fields]
    dict_key_fields = dict.fromkeys(key_fields, 1)
    key_field_ids = [col_id for col_id, col in enumerate(columns_schema)
                     if f"{quote}{col['name']}{quote}" in dict_key_fields]
    return key_fields, key_field_ids


def find_table(check_tables, ck_table_name):
    for table in check_tables:
        # table.TABLE_NAME = target table name
        if table.tgt_table_name.lower() == ck_table_name.lower():
            return table
    return None
