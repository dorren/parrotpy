import pytest

from parrotpy import Parrot
from helpers.spark_helpers import spark

def test_parrot(spark):
    pr = Parrot()

    df = spark.range(5)  \
        .withColumn("name", pr.common.name()) \
        .withColumn("num",  pr.stats.normal(0, 1))

    df.show(3, False)