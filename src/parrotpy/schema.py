from pyspark.sql import Column, DataFrame

from .utils import Snapshot

class DfColumn:
    def __init__(self, name: str, dtype: str, **kwargs: dict):
        self.name = name
        self.dtype = dtype
        self.kwargs = kwargs

    def generate(self, df: DataFrame) -> DataFrame:
        col_val = self.col_fn(**self.args)
        df = df.withColumn(self.name, col_val.cast(self.dtype))
        
    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.dtype,
            "gen":  self.gen
        }
        result = {**result, **self.kwargs}
        return result

class ComputedColumn(DfColumn):
    def __init__(self, name: str, dtype: str, col_val: Column):
        self.name = name
        self.dtype = dtype
        self.col_val = col_val

    def generate(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(self.name, self.col_val.cast(self.dtype))
        return df

    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.dtype,
            "gen":  'TBD'
        }
        return result

class SnapshotColumn(DfColumn):
    def __init__(self, name: str, dtype: str, ss: Snapshot):
        self.name = name
        self.dtype = dtype
        self.snapshot = ss

    def generate(self, df):
        df = df.withColumn(self.name, self.snapshot.result.cast(self.dtype))
        return df
    
    def to_dict(self):
        result = {
            "name": self.name,
            "type": self.dtype
        }
        combined = {**result, **self.snapshot.to_dict()}

        return combined
    
    def to_code(self):
        pass

class DfSchema:
    def __init__(self):
        self.columns = []
    
    def add_column(self, col: DfColumn):
        self.columns.append(col)
        return self

    def to_dict(self):
        result = {}
        result["columns"] = [col.to_dict() for col in self.columns]

        return result
