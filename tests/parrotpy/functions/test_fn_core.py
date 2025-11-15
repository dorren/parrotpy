import pytest

from pyspark.sql import functions as F
from pyspark.testing import assertDataFrameEqual

from parrotpy.functions.core import *

def test_empty_df(parrot):
    n = 10
    df = parrot.empty_df(n)

    assert df.columns == [], f"Expected no columns, got {df.columns}"
    assert df.count() == n, f"Expected {n} rows, got {df.count()}"

def test_auto_increment(spark, parrot):
    n = 10

    df = (parrot.empty_df(n)
        .withColumn("id", auto_increment(start=1000, step=10))
    )

    ids = [(1000 + i * 10,) for i in range(n)]
    expected_df = spark.createDataFrame(ids, "id INT")
    assertDataFrameEqual(df.select("id"), expected_df)


def test_rand_str(spark):
    n = 1000
    df = spark.range(n)
    df = df.withColumn("s", rand_str(3))
    # df.groupBy("s").count().orderBy(F.desc("count")).show(100, False)
    # df.show(10, False)
    assert df.count() == n

def test_rand_num(spark):
    n = 1000
    df = spark.range(n)
    df = df.withColumn("n", rand_num_str(2))
    # df.groupBy("n").count().orderBy("n").show(26, False)
    # df.show(10, False)
    assert df.count() == n

def test_license_plate(spark):
    n = 1000
    df = spark.range(n)
    df = df.withColumn("plate", F.concat(rand_str(3), F.lit("-"), rand_num_str(4)))
    df.show(20, False)
    assert df.count() == n

def test_date_between(spark):
    df = spark.range(10)
    start_str = "2025-11-14"
    end_str   = "2026-11-14"

    df = df.withColumn("create_date", date_between(start_str, end_str))
    assert(df.count() == 10)

def test_timestamp_between(spark):
    df = spark.range(10)
    start_str = "2025-11-14 00:00:00"
    end_str   = "2025-11-15 00:00:00"

    df = df.withColumn("create_time", timestamp_between(start_str, end_str))
    assert(df.count() == 10)