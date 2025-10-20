import pytest
from pyspark.sql import Column

from parrotpy import Parrot
from helpers.spark_helpers import spark

@pytest.mark.skip
def test_parrot(spark):
    pr = Parrot()

    df = spark.range(5)  \
        .withColumn("name", pr.common.name()) \
        .withColumn("num",  pr.stats.normal(0, 1))

    df.show(3, False)

def test_get_attr(spark):
    pr = Parrot()

    s = "parrotpy.stats.normal"
    fn = getattr(__import__(s.rsplit(".",1)[0], fromlist=[s.rsplit(".",1)[1]]), s.rsplit(".",1)[1])

    args = [0,1]
    kwargs = {"seed":1}
    actual = fn(*args, **kwargs)
    assert isinstance(actual, Column)
