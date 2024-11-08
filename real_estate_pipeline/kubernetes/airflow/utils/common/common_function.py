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

def generate_file_name_mysql(
        source,
        dimenstions_table,
        table_name,
        BUSINESS_DATE,
        is_fact=False,
        isParquet=False,
        chunk=None,
        server=None,
        codec=None,
):
    if is_fact:
        # generate chunk with prefix "_"
        chunk = f"_{chunk}" if chunk is not None else ""

        # generate server with prefix "_"
        server = f"_{server}" if server is not None else ""

        # generate codec with prefix "."
        codec = f".{codec}" if codec is not None else ""

        # generate and return filename
        if isParquet:
            filename = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{chunk}{server}_{BRACES}{codec}.parquet"
        else:
            filename = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{chunk}{server}_{BRACES}.json{codec}"
        return filename
    else:
        # generate chunk with prefix "_"
        chunk = f"_{chunk}" if chunk is not None else ""

        # generate server with prefix "_"
        server = f"_{server}" if server is not None else ""

        # generate codec with prefix "."
        codec = f".{codec}" if codec is not None else ""

        # generate and return filename
        if isParquet:
            filename = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{chunk}{server}_{BRACES}{codec}.parquet"
        else:
            filename = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{chunk}{server}_{BRACES}.json{codec}"
        return filename

def get_source_uris_mysql(
        source,
        dimenstions_table,
        table_name,
        BUSINESS_DATE,
        is_fact=False,
        isParquet=False,
        server=None,
        codec=None
):
    # generate server with prefix "_"
    server = f"_{server}" if server is not None else ""

    # generate codec with prefix "."
    codec = f".{codec}" if codec is not None else ""
    if is_fact:
        # generate and return source_uris
        if isParquet:
            source_uris = [
                f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{server}_{ASTERISK}{codec}.parquet"
            ]
        else:
            source_uris = [
                f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{server}_{ASTERISK}.json{codec}"
            ]
        return source_uris
    else:
        if isParquet:
            source_uris = [
                f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{server}_{ASTERISK}{codec}.parquet"
            ]
        else:
            source_uris = [
                f"{dimenstions_table}/{source}/{BUSINESS_DATE}/{table_name}/{table_name}{server}_{ASTERISK}.json{codec}"
            ]
        return source_uris


###############
def generate_file_name_v2(
    source, dimenstions_table, table_name, BUSINESS_DATE, isParquet=False, chunk=None, server_key=None, codec=None,
):
    # generate chunk with prefix "_"
    chunk = f"_{chunk}" if chunk is not None else ""

    # generate server with prefix "_"
    server_key = server_key if server_key is not None else ""

    # generate codec with prefix "."
    codec = f".{codec}" if codec is not None else ""

    # generate and return filename
    if isParquet:
        filename = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/shard_{server_key}/{table_name}{chunk}/{table_name}_{BRACES}{codec}.parquet"

    else:
        filename = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/shard_{server_key}/{table_name}{chunk}/{table_name}_{BRACES}.json{codec}"
    return filename

def get_source_uris_v2(source,
                       dimenstions_table,
                       table_name,
                       BUSINESS_DATE,
                       isParquet=False,
                       server_keys=None,
                       codec=None,
                       chunk=None):
    source_uris = []
    if server_keys is None:
        server_keys = [0]

    chunk = f"_{chunk}" if chunk is not None else ""
    # generate codec with prefix "."
    codec = f".{codec}" if codec is not None else ""
    for server in server_keys:
        # generate and return source_uris
        if isParquet:
            file_uris = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/shard_{server}/{table_name}{chunk}/{table_name}_{ASTERISK}{codec}.parquet"
        else:
            file_uris = f"{dimenstions_table}/{source}/{BUSINESS_DATE}/shard_{server}/{table_name}{chunk}/{table_name}_{ASTERISK}.json{codec}"
        source_uris.append(file_uris)

    return source_uris

def get_param(variables: Dict[str, Any], key: str) -> Any:
    param = variables.get(key)

    if not param:
        raise Exception(f"Can not get {key} from airflow variable")

    return param
