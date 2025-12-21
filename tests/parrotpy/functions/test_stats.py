
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

from helpers.spark_helpers import spark
from parrotpy.functions.stats import normal, mixed_normal
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

def test_mixed_normal(spark):
    """Test that the mixture_normal function generates values approximating the combined distribution."""
    row_count = 1000
    params = [
        {'mean': 10,  'stddev': 2,  'prob': 0.3},
        {'mean': 50,  'stddev': 10, 'prob': 0.5},
        {'mean': 100, 'stddev': 5,  'prob': 0.2}
    ]
    seed = 123
    ctx = {"column_name": "mixed_norm"}

    gen_fn = mixed_normal(params, seed=seed)
    df = spark.range(row_count)
    df = gen_fn(df, ctx)
    assert df.count() == row_count, "Row count does not match"

    # view histogram in R
    #
    # rows = (df
    #     .select("mixed_norm")
    #     .withColumn("mixed_norm", F.round("mixed_norm", F.lit(2)))
    #     .collect()
    # )
    # values = [r["mixed_norm"] for r in rows]

    # print(values[0:500])
    # print()
    # print(values[500:])

    # in R
    # x1 = c(...)
    # x2 = c(...)
    # x=unlist(c(x1, x2))
    # hist(x, breaks=100)