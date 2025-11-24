import pytest

import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from parrotpy import functions as PF
from helpers.test_helpers import benchmark


def test_empty_df(parrot):
    n = 10
    df = parrot.df_builder().empty_df(n)

    assert df.columns == [], f"Expected no columns, got {df.columns}"
    assert df.count() == n, f"Expected {n} rows, got {df.count()}"

def test_auto_increment(spark, parrot):
    n = 10

    df = (parrot.df_builder().empty_df(n)
        .withColumn("id", PF.auto_increment(start=1000, step=10))
    )

    ids = [(1000 + i * 10,) for i in range(n)]
    expected_df = spark.createDataFrame(ids, "id INT")
    assertDataFrameEqual(df.select("id"), expected_df)


def test_rand_str(spark):
    n = 1000
    df = spark.range(n)
    df = df.withColumn("s", PF.rand_str(3))
    # df.groupBy("s").count().orderBy(F.desc("count")).show(100, False)
    # df.show(10, False)
    assert df.count() == n

def test_regex_str(spark):
    n = 10
    df = spark.range(n)
    pattern = F.lit(r"[A-Z]{3}-[0-9]{4}")
    df = df.withColumn("s", PF.regex_str(pattern))
    df.show(10, False)
    assert df.count() == n

def test_rand_num(spark):
    n = 1000
    df = spark.range(n)
    df = df.withColumn("n", PF.rand_num_str(2))
    # df.groupBy("n").count().orderBy("n").show(26, False)
    # df.show(10, False)
    assert df.count() == n

def test_license_plate(spark):
    n = 1000
    df = spark.range(n)
    df = df.withColumn("plate", F.concat(PF.rand_str(3), F.lit("-"), PF.rand_num_str(4)))
    df.show(20, False)
    assert df.count() == n

def test_date_between(spark):
    df = spark.range(10)
    start_str = "2025-11-14"
    end_str   = "2026-11-14"

    df = df.withColumn("create_date", PF.date_between(start_str, end_str))
    assert(df.count() == 10)

def test_timestamp_between(spark):
    df = spark.range(10)
    start_str = "2025-11-14 00:00:00"
    end_str   = "2025-11-15 00:00:00"

    df = df.withColumn("create_time", PF.timestamp_between(start_str, end_str))
    assert(df.count() == 10)

def test_fk(spark):
    ref_df = spark.range(10).withColumnRenamed("id", "fk_id")
    df = spark.range(1000)

    df2 = PF._fk_references(df, ref_df, "fk_id", "fk_id2")
    freq = (df2.groupBy("fk_id2").count()
        .agg(F.mean("count").cast("int").alias("mean"))
        .collect()[0][0]
    )
    assert(freq == 1000/10)


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
    elements = ['A', 'B', 'C', 'D', 'E']
    row_count = 10000
    seed = 42
    col_name = "choice"

    df = spark.range(row_count)
    df = df.withColumn("selected", PF.core._uniform_choice(elements))
    df.groupBy("selected").count().show(5, False)

def test__weighted_choice(spark):
    elements = ['A', 'B', 'C', 'D', 'E']
    weights =  [0.1, 0.2, 0.3, 0.3, 0.1]
    row_count = 10000

    df = spark.range(row_count)
    df = df \
        .withColumn("rnd", F.rand()) \
        .withColumn("selected", PF.core._weighted_choice(elements, weights, "rnd"))

    df.groupBy("selected").count().orderBy("selected").show(5, False)

def test_weighted_choices(parrot):
    elements = ['A', 'B', 'C', 'D', 'E']
    weights =  [0.1, 0.2, 0.3, 0.3, 0.1]
    row_count = 10000

    df = (parrot.df_builder()
        .options(name="letters")
        .build_column("letter", "string", PF.weighted_choices(elements, weights))
        .generate(row_count)
    )

    df.groupBy("letter").count().orderBy("letter").show(5, False)