from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType

from parrotpy import core
from parrotpy.stats import normal
from .schema import ColumnSchema

class Parrot:
    id_col_name = "_id"

    def __init__(self, spark: SparkSession):
        lib_name = __package__  # parrotpy
        self.spark = spark

        modules = ["common", "stats"]
        for mod in modules:
            mod_name = f"{lib_name}.{mod}"
            setattr(self, mod, __import__(mod_name, fromlist=[""]))

    def empty_df(self, n: int):
        """Create an empty dataframe with n rows.

        Args:
            n (int): Number of rows.

        Returns:
            DataFrame: Spark DataFrame.
        """
        return core.empty_df(self.spark, n)
    
    def auto_increment(self, start: int = 0, step: int = 1):
        """Generate an auto-incrementing column starting from `start` with a given `step`.

        Args:
            start (int, optional): Starting value. Defaults to 0.
        """
        return core.auto_increment(start, step)


    def gen_df(self, schema: StructType, row_count: int):
        df = self.spark.range(row_count) \
          .withColumnRenamed("id", "_id")
        
        for field in schema.fields:
            new_col = self.build_column(field)
            df = df.withColumn(field.name, new_col.cast(field.dataType))

        df = df.drop(self.id_col_name)
        return df
    
    def build_column(self, col_schema: ColumnSchema):
        cs = col_schema

        if cs.get("distribution") == "normal":
            return normal(cs.get("mean", 0), cs.get("stddev", 1), seed=cs.get("seed", None))
        else:
            raise ValueError(f"Unsupported distribution: {cs.get('distribution')}")