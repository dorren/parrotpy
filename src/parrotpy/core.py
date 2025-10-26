from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, DateType, DoubleType, LongType, StringType
from pyspark.sql.window import Window

def empty_df(spark: SparkSession, n: int):
    """Create an empty dataframe with n rows.

    Args:
        n (int): Number of rows.

    Returns:
        DataFrame: Spark DataFrame.
    """
    df = spark.range(n).drop("id")
    return df

def auto_increment(start: int = 0, step: int = 1) -> Column:
    """Generate an auto-incrementing column starting from `start` with a given `step`.

    Args:
        start (int, optional): Starting value. Defaults to 0.

    Returns:
        Dataframe: df with new auto-incremented Column.
    """
    return F.expr(f"{start} + (row_number() OVER (ORDER BY (SELECT NULL)) - 1) * {step}")

