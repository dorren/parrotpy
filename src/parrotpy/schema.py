from pyspark.sql import DataFrame

class TableSchema:
    def __init__(self, name: str, columns: list):
        self.name = name
        self.columns = columns

class BaseColumn:
    def __init__(self, name: str, dtype: str, **kwargs: dict):
        self.name = name
        self.dtype = dtype

    def generate(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError("Subclasses must implement the generate method")

    def to_dict(self):
        raise NotImplementedError("Subclasses must implement the to_dict method")



