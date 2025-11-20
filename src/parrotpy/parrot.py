from collections import UserDict
from pyspark.sql import SparkSession
from typing import Any

import parrotpy.functions as PF
from .df_builder import DfBuilder
from .inference.analyzer import Analyzer
from .inference.entity_map import EntityMap

class GeneratedDF(UserDict):
    pass

class Parrot:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.entity_map = EntityMap.default()
        self.dataframes = GeneratedDF()
    
    def df_builder(self) -> DfBuilder:
        return DfBuilder(parrot=self)
    
    def analyzer(self) -> Analyzer:
        return Analyzer(parrot=self)

