from pyspark.sql import Column, DataFrame

from .utils import Snapshot

class ComputedColumn:
    def __init__(self, name: str, data_type: str, col_val: Column):
        self.name = name
        self.data_type = data_type
        self.col_val = col_val

    def generate(self, df: DataFrame, df_builder=None) -> DataFrame:
        df = df.withColumn(self.name, self.col_val.cast(self.data_type))
        return df

    def to_dict(self):
        return self.__dict__

class SnapshotColumn:
    def __init__(self, name: str, data_type: str, ss: Snapshot):
        self.name = name
        self.data_type = data_type
        self.snapshot = ss

    def generate(self, df: DataFrame, df_builder=None):
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



class DfSpec:
    def __init__(self):
        self.columns = []
        self.spec_options = {}
    
    def options(self, **kwargs):
        allowed = ["name", "format"]
        filtered_dict = {key: kwargs[key] for key in kwargs if key in allowed}

        self.spec_options = {**self.spec_options, **filtered_dict}

    def add_column(self, col: ComputedColumn):
        self.columns.append(col)
        return self

    def to_dict(self):
        result = self.spec_options
        result["columns"] = [col.to_dict() for col in self.columns]

        return result
