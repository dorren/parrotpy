from faker import Faker
import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, StringType

from parrotpy.common import name, address
from helpers.spark_helpers import spark

@pytest.fixture(scope="module")
def faker():
    Faker.seed(1)
    faker = Faker()
    return faker

def test_faker_name(faker):
    n = 5
    names = [faker.name() for _ in range(n)]
    assert len(list(set(names))) == n, "Generated duplicate names"
    
def test_df(spark):
    df = spark.range(10)  \
        .withColumn("name", name()) \
        .withColumn("address", address())
    df.show(3, False)

    names_df = df.select("name", "address").distinct()
    assert df.count() == names_df.count(), "Generated duplicate names in DataFrame"
