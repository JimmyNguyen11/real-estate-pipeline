import os
import sys
abs_path = os.path.dirname(os.path.abspath(__file__)) + '/../..'
sys.path.append(abs_path)

RAW = "raw"
STAGING = "staging"
WAREHOUSE = "warehouse"
MART = "mart"

ICEBERG = 'iceberg'
PARQUET_FORMAT = 'parquet'

BUCKET = 'kvconnect'
KVCONNECT_LAKEHOUSE = 'kvconnect_lakehouse'
BUCKET_RAW = 'kvconnect_raw'
BUCKET_STAGING = 'kvconnect_staging'
BUCKET_WAREHOUSE = 'kvconnect_warehouse'



KVCRM = 'kvcrm'
KVCRM_SQL = 'kvcrm'
KVCRM_BUCKET_LAKEHOUSE = 'kvcrm_lakehouse'
KVCRM_BUCKET_RAW = 'kvcrm_raw'
KVCRM_BUCKET_STAGING = 'kvcrm_staging'
KVCRM_BUCKET_WAREHOUSE = 'kvcrm_warehouse'


KVHRM = 'kvhrm'
KVHRM_BUCKET_LAKEHOUSE = 'kvhrm_lakehouse'
KVHRM_BUCKET_RAW = 'kvhrm_raw'
KVHRM_BUCKET_STAGING = 'kvhrm_staging'
KVHRM_BUCKET_WAREHOUSE = 'kvhrm_warehouse'


KIOTFINANCE_CRM = 'kiotfinance_crm'
KIOTFINANCE_CRM_BUCKET_LAKEHOUSE = 'kiotfinance_crm_lakehouse'
KIOTFINANCE_CRM_BUCKET_RAW = 'kiotfinance_crm_raw'
KIOTFINANCE_CRM_BUCKET_STAGING = 'kiotfinance_crm_staging'
KIOTFINANCE_CRM_BUCKET_WAREHOUSE = 'kiotfinance_crm_warehouse'