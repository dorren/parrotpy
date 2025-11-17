from faker import Faker
import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, StringType

from parrotpy import functions as PF
# from parrotpy.functions.common import person_name, address
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
        .withColumn("name",    PF.common.person_name()) \
        .withColumn("address", PF.common.address())
    df.show(3, False)

    names_df = df.select("name", "address").distinct()
    assert df.count() == names_df.count(), "Generated duplicate names in DataFrame"


def test_expr(spark):

    def fake_name():
        faker = Faker()
        return faker.name()
    
    name_udf = udf(fake_name, StringType())
    spark.udf.register("fake_name", name_udf)

    df = spark.range(5)
    df.createOrReplaceTempView("test_table")
    df = spark.sql("""
        SELECT id, fake_name() AS my_name
        FROM test_table
    """)
    # df = df.withColumn("my_name", F.expr("fake_name()"))
    df.show(3, False)