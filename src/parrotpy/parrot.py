from pyspark.sql import SparkSession
from typing import Any

import parrotpy.functions as PF
from .df_builder import DfBuilder
from .inference.analyzer import Analyzer
from .inference.entity_map import EntityMap

class Parrot:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.entity_map = EntityMap.default()

    def empty_df(self, n: int):
        """Create an empty dataframe with n rows.

        Args:
            n (int): Number of rows.

        Returns:
            DataFrame: Spark DataFrame.
        """
        df = self.spark.range(n).drop("id")
        return df
    
    def df_builder(self) -> DfBuilder:
        return DfBuilder(parrot=self)
    
    def analyzer(self) -> Analyzer:
        return Analyzer(parrot=self)

    def gen_df(self, df_spec, n: int):
        df = self.empty_df(n)

        for col in df_spec.columns:
            df = col.generate(df)

        return df
