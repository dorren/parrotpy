from collections import UserDict
from pyspark.sql import SparkSession
from typing import Any

from .schema_builder import SchemaBuilder
from .analyzer import Analyzer

class Context(UserDict):
    def register(name: str, fn:callable):
        super().__setitem__(name, fn)

class Parrot:
    def __init__(self, spark: SparkSession):
        self.spark = spark

        # lib_name = __package__  # parrotpy
        # modules = ["common", "stats"]
        # for mod in modules:
        #     mod_name = f"{lib_name}.{mod}"
        #     setattr(self, mod, __import__(mod_name, fromlist=[""]))


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
        return SchemaBuilder(self)
    
    def analyzer(self) -> Analyzer:
        return Analyzer()


