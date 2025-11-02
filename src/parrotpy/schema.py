from pyspark.sql import DataFrame

            
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
