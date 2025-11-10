from pyspark.sql import SparkSession
from typing import Any

import parrotpy.functions as PF
from .schema_builder import SchemaBuilder
from .analyzer import Analyzer
from .function_map import FunctionMap

class Parrot:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self._bootup()

    def _bootup(self):
        self.fn_map = FunctionMap()
        self.fn_map.register("distribution.norm",    PF.stats.normal)
        self.fn_map.register("distribution.uniform", PF.stats.uniform)

    def empty_df(self, n: int):
        """Create an empty dataframe with n rows.

        Args:
            n (int): Number of rows.

        Returns:
            DataFrame: Spark DataFrame.
        """
        df = self.spark.range(n).drop("id")
        return df
    
    def schema_builder(self):
        return SchemaBuilder(parrot=self)
    
    def analyzer(self) -> Analyzer:
        return Analyzer(parrot=self)

    def gen_df(self, schema, n: int):
        df = self.empty_df(n)

        for col in schema.columns:
            df = col.generate(df)

        return df
