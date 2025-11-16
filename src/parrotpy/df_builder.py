from typing import Any
from pyspark.sql import DataFrame

from parrotpy.functions.stats import normal
from .functions.core import ForeignKey, ForeignKeyColumn
from .df_spec import DfSpec, ComputedColumn, Snapshot, SnapshotColumn
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

    def build_from_dict(self, name: str, dtype: str, kwargs: dict):
        entity_map = self.parrot.entity_map
        fn = entity_map.match(kwargs)
        if fn:
            del kwargs["distribution"]
            fn_ss = snapshot(fn)(**kwargs)
            col = SnapshotColumn(name, dtype, fn_ss)
            self.df_spec.add_column(col)
        else:
            raise ValueError(f"Can find function for given attributes {kwargs}")

        # if "gen" in kwargs:
        #     del kwargs["gen"]
        #     fn_ss = snapshot(normal)(**kwargs)  # TODO, call fn by name
        #     col = SnapshotColumn(name, dtype, fn_ss)
        #     self.schema.add_column(col)

        return self
    
    def build_column(self, name: str, dtype: str, col_value:Any = None, **kwargs: dict):
        """Build a column based on the provided attributes.

        Args:
            name (str): Column name.
            dtype (str): Data type of the column.
            attrs (dict): Attributes for the value generating function.

        Returns:
            Column: Spark Column.
        """
        if col_value is not None:
            if type(col_value).__name__ == "Column":
                col = ComputedColumn(name, dtype, col_value)
                self.df_spec.add_column(col)
            elif type(col_value) is Snapshot:
                col = SnapshotColumn(name, dtype, col_value)
                self.df_spec.add_column(col)
            elif type(col_value) is ForeignKey:
                col = ForeignKeyColumn(name, dtype, col_value)
                self.df_spec.add_column(col)
                    
        else:
            self.build_from_dict(name, dtype, kwargs)

        return self

    def find_df(self, df_name: str) -> DataFrame:
        """ find generated DF """
        return self.parrot.generated_df.get(df_name)

    def gen_df(self, row_count: int):
        df = self.empty_df(row_count)

        for col in self.df_spec.columns:
            df = col.generate(df, df_builder=self)

        self.register_df(self.df_spec.spec_options["name"], df)

        return df

    def register_df(self, name: str, df: DataFrame):
        self.parrot.generated_df[name] = df