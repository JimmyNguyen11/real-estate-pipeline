# set vars for iceberg property name
write_format_default = 'write.format.default'
write_parquet_compression_codec = 'write.parquet.compression-codec'
format_version = 'format-version'
previous_versions_max = 'write.metadata.previous-versions-max'
external_table_purge = 'external.table.purge'
target_file_size_bytes = 'write.target-file-size-bytes'


def get_general_iceberg_table_props():
    """
    return general_iceberg_table_props
    """
    iceberg_table_props = dict()
    iceberg_table_props[write_format_default] = 'parquet'
    iceberg_table_props[write_parquet_compression_codec] = 'snappy'
    iceberg_table_props[format_version] = '1'
    iceberg_table_props[previous_versions_max] = '2'
    iceberg_table_props[external_table_purge] = 'true'
    iceberg_table_props[target_file_size_bytes] = '134217728'
    return iceberg_table_props