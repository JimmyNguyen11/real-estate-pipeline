from typing import Any, Dict

BRACES = "{}"
ASTERISK = "*"

def generate_file_name(
    source, dimenstions_table, table_name, BUSINESS_DATE, isParquet=False, chunk=None, server=None, codec=None,
):
    # generate chunk with prefix "_"
    chunk = f"_{chunk}" if chunk is not None else ""

    # generate server with prefix "_"
    server = f"_{server}" if server is not None else ""

    # generate codec with prefix "."
    codec = f".{codec}" if codec is not None else ""

    # generate and return filename
    if isParquet:
        filename = f"{dimenstions_table}/{source}_{table_name}/{table_name}_{BUSINESS_DATE}{chunk}{server}_{BRACES}{codec}.parquet"
    else:
        filename = f"{dimenstions_table}/{source}_{table_name}/{table_name}_{BUSINESS_DATE}{chunk}{server}_{BRACES}.json{codec}"
    return filename


def get_source_uris(source, dimenstions_table, table_name, BUSINESS_DATE, isParquet=False, server=None, codec=None):
    # generate server with prefix "_"
    server = f"_{server}" if server is not None else ""

    # generate codec with prefix "."
    codec = f".{codec}" if codec is not None else ""

    # generate and return source_uris
    if isParquet:
        source_uris = [
            f"{dimenstions_table}/{source}_{table_name}/{table_name}_{BUSINESS_DATE}{server}_{ASTERISK}{codec}.parquet"
        ]
    else:
        source_uris = [
            f"{dimenstions_table}/{source}_{table_name}/{table_name}_{BUSINESS_DATE}{server}_{ASTERISK}.json{codec}"
        ]
    return source_uris
