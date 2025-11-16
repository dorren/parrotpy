from abc import ABC, abstractmethod
from pyspark.sql import Column, DataFrame
from typing import Any

from .utils import Snapshot


class ColumnLike(ABC):
    def __init__(self, name: str, data_type: str, col_val: Any):
        self.name = name
        self.data_type = data_type
        self.col_val = col_val

    @abstractmethod
    def generate(self, df: DataFrame, df_builder=None) -> DataFrame:
        pass

    def __str__(self):
        cls_name = self.__class__.__name__
        return f"{cls_name}({self.__dict__})"
    
class NativeColumn(ColumnLike):
    """ column value is a native spark.sql.Column """
    def generate(self, df: DataFrame, df_builder=None) -> DataFrame:
        df = df.withColumn(self.name, self.col_val.cast(self.data_type))
        return df

class CustomColumn(ColumnLike):
    """ column value is a native spark.sql.Column """
    def generate(self, df: DataFrame, df_builder=None) -> DataFrame:
        pass

class SnapshotColumn(ColumnLike):
    def __init__(self, name: str, data_type: str, col_val: Snapshot):
        super().__init__(name, data_type, col_val)

    def generate(self, df: DataFrame, df_builder=None):
        ctx = {"df": df, "df_builder": df_builder}
        computed_value = self.col_val.invoke(ctx)

        df = df.withColumn(self.name, computed_value.cast(self.data_type))
        return df

class ForeignKeyColumn(ColumnLike):
    def generate(self, df: DataFrame, df_builder):
        df_name, fk_col_name = self.col_val.path.split(".")
        fk_df = df_builder.find_df(df_name)
        
        df2 = self.col_val.references(df, fk_df, fk_col_name, self.name)
        return df2

class DfSpec:
    def __init__(self):
        self.columns = []
        self.spec_options = {}
    
    def options(self, **kwargs):
        allowed = ["name", "format"]
        filtered_dict = {key: kwargs[key] for key in kwargs if key in allowed}

        self.spec_options = {**self.spec_options, **filtered_dict}

    def add_column(self, col: NativeColumn):
        self.columns.append(col)
        return self
    
    def __str__(self):
        result = f"{self.__class__}({self.spec_options})"
        for c in self.columns:
            result += f"\n  {c}"

        return result
