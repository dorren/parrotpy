
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from helpers.spark_helpers import spark
from parrotpy.functions.stats import normal
from helpers.test_helpers import benchmark


def test_normal_distribution(spark):
    """Test that the normal function generates values with expected mean and standard deviation."""
    row_count = 100
    mean = 100
    stddev = 5.0
    new_col_name = "norm"

    df = spark.range(row_count)
    new_col = normal(1, mean, stddev, seed=42)
    df = df.withColumn(new_col_name, new_col)
    df.show(3, False)

    stats = df.select(
        F.mean(new_col_name).alias("mean"),
        F.stddev(new_col_name).alias("stddev")
    ).collect()[0]
    
    actual_mean = stats["mean"]
    actual_stddev = stats["stddev"]
    assert abs(actual_mean/mean - 1) < 0.05,    f"Mean {actual_mean} not within 5% of expected {mean}"
    assert abs(actual_stddev/stddev - 1) < 0.1, f"StdDev {actual_stddev} not within 10% of expected {stddev}"

def test_normal_array(spark):
    row_count = 1000
    array_size = 10
    mean = 100
    stddev = 5.0
    seed = 42
    col_name = "norm_arr"

    samples = normal(array_size, mean, stddev, seed=seed)

    df = spark.range(row_count)
    with benchmark("Generate normal array"):
      df = df.withColumn(col_name, samples)
      rows = df.collect()
    df.show(3, False)

def test_normal_array_no_seed(spark):
    row_count = 5
    mean = 100
    stddev = 5.0
    array_size = 3 
    col_name = "norm_arr"

    samples = normal(array_size, mean, stddev)
    df = spark.range(row_count)
    df = df.withColumn(col_name, samples)
    df.show(3, False)
