from schema.common.model import BaseModel
from schema.common.dao_dim import DaoDim

class Demo(DaoDim, BaseModel):
    def __init__(self, table_name):
        super().__init__(table_name)
        self.CONNECTION_TYPE = "MYSQL"
        self.COLUMNS = [
            {"name": "du_an", "mode": "NULLABLE", "type": "STRING"},
            {"name": "price_VND", "mode": "NULLABLE", "type": "DOUBLE"},
            {"name": "location", "mode": "NULLABLE", "type": "STRING"},
            {"name": "status", "mode": "NULLABLE", "type": "STRING"},
            {"name": "area", "mode": "NULLABLE", "type": "DOUBLE"},
            {"name": "price_m2", "mode": "NULLABLE", "type": "DOUBLE"},
            {"name": "toilet", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "room", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "doc", "mode": "NULLABLE", "type": "STRING"},
            {"name": "type", "mode": "NULLABLE", "type": "STRING"},
            {"name": "So_tang", "mode": "NULLABLE", "type": "BIGINT"},
            {"name": "furnishing", "mode": "NULLABLE", "type": "STRING"},
        ]
        self.COLUMNS_SCHEMA = self.COLUMNS
        self.TABLE_TYPE = 'DIM'

class TableDemoDLK:
    demo = Demo('demo')

    ALL_TABLES = [
        demo
    ]