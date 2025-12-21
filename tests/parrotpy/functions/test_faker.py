from faker import Faker
import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, DoubleType, StringType, MapType
from pyspark.testing import assertDataFrameEqual

from parrotpy import functions as PF
from parrotpy.functions import rand_array
from helpers.spark_helpers import spark, assert_df_equal
from helpers.test_helpers import benchmark

@pytest.fixture(scope="module")
def faker():
    Faker.seed(1)
    faker = Faker()
    return faker

def test_faker_name(faker):
    n = 5
    names = [faker.name() for _ in range(n)]
    print(names)
    assert len(list(set(names))) == n, "Generated duplicate names"

def test_lit(spark, faker):
    df = (spark.range(5)
        .withColumn("test",    F.lit("lit")) 
        .withColumn("name",    F.lit(faker.name()))
        .withColumn("address", F.lit(faker.address())))
    
    unique_count = df.select("name", "address").distinct().count()
    assert unique_count == 1, "Expected only one distinct row"
    
def test_df(spark):
    @udf(returnType=StringType())
    def name_udf():
        faker = Faker()
        return faker.name()

    Faker.seed(1)
    df = (spark.range(5)  \
        .withColumn("test",    F.lit("name_udf")) 
        .withColumn("name",    name_udf()))
    df.show(5, False)

    names_df = df.select("name").distinct()
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
        SELECT id, "expr" AS test, fake_name() AS my_name
        FROM test_table
    """)
    df.show(5, False)


def test_name_w_seed(parrot):
    row_count = 5
    seed = 100

    df1 = (parrot.df_builder()
        .build_column("name", "string", PF.faker("name", seed))
        .generate(row_count)
    )
    df2 = (parrot.df_builder()
        .build_column("name", "string", PF.faker("name", seed))
        .generate(row_count)
    )

    assert_df_equal(df1, df2)

def test_common_fn(parrot):
    row_count = 5
    seed = 100

    df1 = (parrot.df_builder()
        .build_column("name", "string", PF.common.person_name(seed))
        .generate(row_count)
    )
    df2 = (parrot.df_builder()
        .build_column("name", "string", PF.common.person_name(seed))
        .generate(row_count)
    )
    
    assert_df_equal(df1, df2)


def test_name_array_no_seed(parrot):
    row_count = 5
    array_size = 3

    df = (parrot.df_builder()
        .build_column("names", "array<string>", PF.faker_array(array_size, "name"))
        .generate(row_count)
    )

    df.show(row_count, False)
    assert df.count() == row_count

def test_name_array_w_seed(parrot):
    row_count = 100
    array_size = 3
    seed = 100

    df = (parrot.df_builder()
        .build_column("names", "array<string>", PF.faker_array(array_size, "name", 100))
        .generate(row_count)
    )

    df.show(5, False)
    assert df.count() == row_count

def test_name_dict_no_seed(parrot):
    @udf(returnType=MapType(StringType(), StringType()))
    def family():
        faker = Faker()
        return {
            "father":  faker.name_male(),
            "mother":  faker.name_female(),
            "siblings": [faker.name(), faker.name()]
        }
    
    row_count = 3
    df = (parrot.df_builder()
        .options(name="dict_df")
        .build_column("family", "map<string,string>", family())
        .generate(row_count)
    )

    df.show(row_count, False)
    assert df.count() == row_count