from pyspark.sql import Column, DataFrame
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F


def normal(mean: float, sd: float, to_int:bool=False, seed:int=None) -> Column:
    """Generate a spark column with sample value from a normal distribution.

    Args:
        mean (float): The mean of the normal distribution.
        sd (float): The standard deviation of the normal distribution.
        to_int (bool, optional): Whether to return integer value. Defaults to False.
        seed (int, optional): randomization seed.

    Returns:
        Column: Spark Column.
    """
    value = F.randn(seed) * sd + mean
    value = F.rint(value).cast(IntegerType()) if to_int else value

    return value


def add_random_array(df: DataFrame, col_name: str, new_col: Column, array_size: int) -> DataFrame:
    """Add a new column with array values to the DataFrame.

    Spark does not support adding array columns directly, so we
    explode the array values into rows, add the new column, and aggregate

    Args:
        df (DataFrame): Input Spark DataFrame.
        col_name (str): Name of the new column.
        new_col (Column): new array column to add.
        array_size (int): Size of the array.

    Returns:
        DataFrame: DataFrame with the new array column.
    """
    rn = "_rn"        # row number column name
    dummy = "_dummy"  # dummy column name for explode

    # Add row number to uniquely identify rows
    df2 =df.withColumn(rn, F.monotonically_increasing_id())

    # explode array values into rows, add new column, and aggregate back into arrays
    df3 = df2.withColumn(
        dummy, F.explode(F.array([F.lit(i) for i in range(array_size)]))) \
        .drop(dummy)                                                      \
        .withColumn(col_name, new_col)                     \
        .groupBy(rn).agg(F.collect_list(col_name).alias(col_name))
        
    return df2.join(df3, on=rn, how='inner').drop(rn)