from typing import Any
from pyspark.sql import Column, DataFrame

from parrotpy.functions.stats import normal
from .df_spec import DfSpec, NativeColumn, CustomColumn, Snapshot, SnapshotColumn
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
    
    def build_column(self, name: str, dtype: str, col_value:Any = None, **kwargs: dict):
        """Build a column based on the provided attributes.

        Args:
            name (str): Column name.
            dtype (str): Data type of the column.
            attrs (dict): Attributes for the value generating function.

        Returns:
            Column: Spark Column.
        """
        if isinstance(col_value, Column):
            col = NativeColumn(name, dtype, col_value)
        elif isinstance(col_value, Snapshot):
            col = SnapshotColumn(name, dtype, col_value)
        else:
            col = CustomColumn(name, dtype, col_value)
        
        self.df_spec.add_column(col)

        return self

    def find_df(self, df_name: str) -> DataFrame:
        """ find generated DF """
        return self.parrot.generated_df.get(df_name)

    def gen_df(self, row_count: int):
        df = self.empty_df(row_count)

        for col in self.df_spec.columns:
            df = col.generate(df, df_builder=self)

        # TODO, this should not be here.
        if "name" in self.df_spec.spec_options:
            self.register_df(self.df_spec.spec_options["name"], df)

        return df

    def register_df(self, name: str, df: DataFrame):
        self.parrot.generated_df[name] = df