from src.utils.constants import HIVE, KUDU


class TableNotFoundError(Exception):

    def __init__(self, table_name: str):
        self.table_name: str = table_name
        self.message = f"Table {table_name} not found."
        super().__init__(self.message)


class InvalidDataSourceError(Exception):

    def __init__(self, data_source: str):
        self.data_source: str = data_source

        allowed_ds = [HIVE, KUDU]
        self.message = f"Data source {data_source} is invalid. Allowed ones are {allowed_ds}."
        super().__init__(self.message)


class NoSchemaRightsError(Exception):

    def __init__(self, schema: str):
        self.schema = schema
        self.message = f"No rights to access {schema}."
        super().__init__(self.message)
