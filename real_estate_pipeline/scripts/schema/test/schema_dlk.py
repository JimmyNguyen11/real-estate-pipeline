from airflow.example_dags.schema.common.model import BaseModel
from airflow.example_dags.schema.common.dao_dim import DaoDim


class Test(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.CONNECTION_TYPE = "MYSQL"
        self.COLUMNS = [
            {"name": "so", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "ten", "mode": "NULLABLE", "type": "STRING"},
            {"name": "ho", "mode": "NULLABLE", "type": "STRING"},
        ]
        self.COLUMNS_SCHEMA = self.COLUMNS
        self.TABLE_TYPE = 'DIM'

class TableTestDLK:
    test = Test('test')

    ALL_TABLES = [
        test
    ]