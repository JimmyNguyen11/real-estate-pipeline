class DaoDim:
    def get_sql_statement_extract_data(
        self,
        BUSINESS_DATE,
        table_name,
        columns,
        schema_name="",
        by_date=None,
        value_date=None,
        ETL_DATE="CURRENT_TIMESTAMP()",
    ):
        column_alias = ["{}.{}".format(table_name, column) for column in columns]
        if by_date is None:
            SQL_SELECT = (
                "SELECT "
                "{3} AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} ".format(
                    BUSINESS_DATE, ",".join(column_alias), table_name, ETL_DATE
                )
            )
        else:
            SQL_SELECT = (
                "SELECT "
                "{4} AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} WHERE {3}".format(
                    BUSINESS_DATE, ",".join(column_alias), table_name, by_date, ETL_DATE
                )
            )
        return SQL_SELECT

    def get_sql_statement_extract_data_v2(
        self,
        BUSINESS_DATE,
        table_name,
        columns,
        server_key,
        schema_name="",
        by_date=None,
        value_date=None,
        ETL_DATE="CURRENT_TIMESTAMP()",
    ):
        column_alias = ["{}.{}".format(table_name, column) for column in columns if column != '"ServerKey"']
        column_alias.append("{} AS ServerKey".format(server_key))

        if by_date is None:
            SQL_SELECT = (
                "SELECT "
                "{3} AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} ".format(
                    BUSINESS_DATE, ",".join(column_alias), table_name, ETL_DATE
                )
            )
        else:
            SQL_SELECT = (
                "SELECT "
                "{4} AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} WHERE {3}".format(
                    BUSINESS_DATE, ",".join(column_alias), table_name, by_date, ETL_DATE
                )
            )
        return SQL_SELECT
    def get_sql_statement_extract_data_my_sql(
        self,
        BUSINESS_DATE,
        table_name,
        columns,
        schema_name="",
        by_date=None,
        value_date=None,
    ):
        if by_date is None:
            SQL_SELECT = (
                "SELECT "
                "now() AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} ".format(BUSINESS_DATE, ",".join(columns), table_name)
            )
        else:
            SQL_SELECT = (
                "SELECT "
                "now() AS ETL_DATE"
                ",'{0}' AS BUSINESS_DATE"
                ",{1} FROM {2} ".format(BUSINESS_DATE, ",".join(columns), table_name)
            )
        return SQL_SELECT

    def get_date_filter_condition_my_sql(
        self, column, operator="=", value_date=None
    ):
        filter_condition = f"CONVERT(varchar, {column}, 23) {operator} '{value_date}'"
        return filter_condition