import logging
from typing import Any
from pyspark.sql import Column, DataFrame

from parrotpy.functions.stats import normal
from .models import DfSpec, ColumnSpec, ColumnValue
from .utils import snapshot

class DfBuilder:
    def __init__(self, parrot):
        self.parrot = parrot
        self.df_spec = DfSpec()
        
    def options(self, **kwargs):
        self.df_spec.options(**kwargs)
        return self

    def empty_df(self, n: int):
        """Create an empty dataframe with n rows.

        Args:
            n (int): Number of rows.

        Returns:
            DataFrame: Spark DataFrame.
        """
        df = self.parrot.spark.range(n).drop("id")
        return df
    
    def build_column(self, name: str, dtype: str, col_value: ColumnValue):
        """Build a column based on the provided attributes.

        Args:
            name (str): Column name.
            dtype (str): Data type of the column.
            col_value (ColumnValue): Attributes or function for the column value.

        Returns:
            DfBuilder: self.
        """
        
        col_spec = ColumnSpec(name, dtype, col_value)
        self.df_spec.add_column(col_spec)

        return self

    def find_df(self, df_name: str) -> DataFrame:
        """ find generated DF """
        return self.parrot.dataframes.get(df_name)

    def generate_column(self, df: DataFrame, col_spec: ColumnSpec, context: dict):
        col_val = col_spec.value
        if isinstance(col_val, Column):
            df = df.withColumn(col_spec.name, col_val.cast(col_spec.data_type))
        elif callable(col_val):
            df = col_val(df, col_spec, context)
        else:
            logging.warning(f"don't know how to handle column {col_spec.name} with value {col_val}")
        
        return df
    
    def _gen_df(self, row_count: int):
        df = self.empty_df(row_count)

        context = {"dataframes": self.parrot.dataframes}
        for col_spec in self.df_spec.columns:
            df = self.generate_column(df, col_spec, context)

        return df

    def register_df(self, name: str, df: DataFrame):
        self.parrot.dataframes[name] = df

    def generate(self, row_count: int):
        df = self._gen_df(row_count)

        if "name" in self.df_spec._options:
            self.register_df(self.df_spec._options["name"], df)

        return df