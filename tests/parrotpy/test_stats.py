import numpy as np
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType

from helpers.spark_helpers import spark
from parrotpy.stats import normal, normal_array
from helpers.test_helpers import benchmark


def test_normal_distribution(spark):
    """Test that the normal function generates values with expected mean and standard deviation."""
    row_count = 100
    mean = 100
    stddev = 5.0
    new_col_name = "norm"

    df = spark.range(row_count)
    new_col = normal(mean, stddev, seed=42)
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

    samples = normal_array(array_size, mean, stddev, seed=seed)

    df = spark.range(row_count)
    with benchmark("Generate normal array"):
      df = df.withColumn(col_name, samples)
      rows = df.collect()
    df.show(3, False)

def test_normal_array_no_seed(spark):
    row_count = 1000
    mean = 100
    stddev = 5.0
    array_size = 10
    seed = 42
    col_name = "norm_arr"

    samples = normal_array(array_size, mean, stddev)
    df = spark.range(row_count)
    df = df.withColumn(col_name, samples)

def test_numpy(spark):
    @udf(returnType=ArrayType(DoubleType()))
    def gen_numpy_array(mean, sd, size, seed):
        # np.random.seed(seed)
        return np.random.normal(mean, sd, size).tolist()

    row_count = 1000
    mean = 100
    stddev = 5.0
    array_size = 10
    seed = 42
    col_name = "numpy_arr"

    df = spark.range(row_count)

    with benchmark("Use numpy.random.normal()"):
      df = df.withColumn(col_name, gen_numpy_array(F.lit(mean), F.lit(stddev), F.lit(array_size), F.lit(seed)))
      rows = df.collect()

def test_py_random_choices():
    import random
    from collections import Counter

    choices = ['A', 'B', 'C', 'D', 'E']
    weights = [0.1, 0.2, 0.3, 0.3, 0.1]
    row_count = 10000
    seed = 42

    random.seed(seed)
    results = random.choices(choices, weights, k=row_count)
    actual = Counter(results)
    print(actual)

def test_uniform_choice(spark):
    from parrotpy.stats import _uniform_choice

    elements = ['A', 'B', 'C', 'D', 'E']
    row_count = 1000
    seed = 42
    col_name = "choice"

    df = spark.range(row_count)
    df = df.withColumn("selected", _uniform_choice(elements))
    df.groupBy("selected").count().show(5, False)

def test_weighted_choice(spark):
    from parrotpy.stats import _weighted_choice

    elements = ['A', 'B', 'C', 'D', 'E']
    weights =  [0.1, 0.2, 0.3, 0.3, 0.1]
    row_count = 10000

    df = spark.range(row_count)
    df = df \
      .withColumn("rand", F.rand()) \
      .withColumn("selected", _weighted_choice(elements, weights, F.col("rand")))

    df.groupBy("selected").count().orderBy("selected").show(5, False)
