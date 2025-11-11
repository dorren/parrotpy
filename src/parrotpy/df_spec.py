from pyspark.sql import Column, DataFrame

from .utils import Snapshot

class ColumnSpec:
    def __init__(self, name: str, data_type: str, entity_type: str="unknown", **kwargs: dict):
        self.name = name
        self.data_type = data_type
        self.entity_type = entity_type
        self.kwargs = kwargs

    def generate(self, df: DataFrame) -> DataFrame:
        col_val = self.col_fn(**self.args)
        df = df.withColumn(self.name, col_val.cast(self.data_type))
        
    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.data_type,
            "entity_type": self.entity_type,
            "gen":  None
        }
        result = {**result, **self.kwargs}
        return result

class ComputedColumn(ColumnSpec):
    def __init__(self, name: str, data_type: str, col_val: Column):
        self.name = name
        self.data_type = data_type
        self.col_val = col_val

    def generate(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(self.name, self.col_val.cast(self.data_type))
        return df

    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.data_type,
            "gen":  None
        }
        return result

class SnapshotColumn(ColumnSpec):
    def __init__(self, name: str, data_type: str, ss: Snapshot):
        self.name = name
        self.data_type = data_type
        self.snapshot = ss

    def generate(self, df):
        col_value = self.snapshot.invoke()
        df = df.withColumn(self.name, col_value.cast(self.data_type))
        return df
    
    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.data_type
        }
        combined = {**result, **self.snapshot.to_dict()}

        return combined
    
    def to_code(self):
        pass

class DfSpec:
    def __init__(self):
        self.columns = []
    
    def add_column(self, col: ColumnSpec):
        self.columns.append(col)
        return self

    def to_dict(self):
        result = {}
        result["columns"] = [col.to_dict() for col in self.columns]

        return result
