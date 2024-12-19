from typing import Any, Dict

BRACES = "{}"
ASTERISK = "*"

def get_source_uris(source, dimenstions_table, table_name, isParquet=False, server=None, codec=None):
    # generate server with prefix "_"
    server = f"_{server}" if server is not None else ""

    # generate codec with prefix "."
    codec = f".{codec}" if codec is not None else ""

    # generate and return source_uris
    if isParquet:
        source_uris = [
            f"{dimenstions_table}/{source}_{table_name}/{table_name}_{ASTERISK}{codec}.parquet"
        ]
    else:
        source_uris = [
            f"{dimenstions_table}/{source}_{table_name}/{table_name}_{ASTERISK}.json{codec}"
        ]
    return source_uris
