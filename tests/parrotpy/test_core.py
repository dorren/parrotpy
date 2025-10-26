import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.testing import assertDataFrameEqual

from parrotpy.core import auto_increment

def test_empty_df(parrot):
    n = 10
    df = parrot.empty_df(n)

    assert df.columns == [], f"Expected no columns, got {df.columns}"
    assert df.count() == n, f"Expected {n} rows, got {df.count()}"

def test_auto_increment(spark, parrot):
    n = 10

    df = (parrot.empty_df(n)
        .withColumn("id", parrot.auto_increment(start=1000, step=10))
    )

    ids = [(1000 + i * 10,) for i in range(n)]
    expected_df = spark.createDataFrame(ids, "id INT")
    assertDataFrameEqual(df.select("id"), expected_df)