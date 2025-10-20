from itertools import accumulate
import json
import pytest
from pprint import pprint
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from random import random

from parrotpy import Parrot
from helpers.spark_helpers import spark

def test_inspect(spark):
    pr = Parrot()

    df = spark.range(5)  \
        .withColumn("name", pr.common.name()) \
        .withColumn("num",  pr.stats.normal(0, 1))

    j_str = df.schema.json()

def test_recreate_schema(spark):
    j_str = '''
        {"fields": [
            {"metadata": {}, "name": "id", "nullable": false, "type": "long"},
            {"metadata": {}, "name": "name", "nullable": true, "type": "string"},
            {
              "metadata": {
                "distribution":"norm",
                "mean": 0.0,
                "std_dev": 1.0,
                "seed": 1234
            }, 
              "name": "num", 
              "nullable": false, 
              "type": "double"
            }
          ],
        "type": "struct"
        }
    '''

    schema2 = StructType.fromJson(json.loads(j_str))
    field = schema2["num"]
    assert "distribution" in field.metadata
    # md = Metadata(field.metadata)
    # print(md)

@pytest.fixture(scope="module")
def categories():
    categories = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
    weights = [0.05, 0.1, 0.15, 0.2, 0.1, 0.1, 0.1, 0.05, 0.05, 0.05]
    cum_weights = accumulate(weights)

    return list(zip(categories, cum_weights))

def test_sample(spark, categories):
    df = spark.createDataFrame(categories, schema=["category", "weight"]) \
      .withColumn("weight", F.round(F.col("weight"), 3))
    
    min_max_df = df.groupBy(F.lit(1)) \
      .agg(F.min("weight").alias("min_w"), 
           F.max("weight").alias("max_w")) \
      .drop("1")
    min_max_df.show()

    df.crossJoin(min_max_df) \
      .withColumn("rand1", F.round(F.rand(), 3)) \
      .show(10, False)
    
def check_result(rnd, df):
    window_spec = Window \
      .orderBy("weight") \
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)        

    df.withColumn("rand", F.round(F.lit(rnd), 3)) \
        .withColumn("selected", F
            .when((F.col("rand") < F.col("min_w")), F.col("min_cat"))
            .when((F.col("rand") > F.col("max_w")), F.col("max_cat"))
            .when((F.col("rand") >= F.col("weight")), F.col("next_cat"))
            .otherwise(F.lit(None))
        ) \
        .filter(F.col("selected").isNotNull()) \
        .select(F.max("selected").alias("final")) \
        .show(10, False)

def test_sample2(spark, categories):
    df = spark.createDataFrame(categories, schema=["category", "weight"]) \
      .withColumn("weight", F.round(F.col("weight"), 3))
    
    window_spec = Window \
      .orderBy("weight") \
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    
    df = df.withColumn("next_cat", F.lead("category").over(Window.orderBy("weight"))) \
            .withColumn("min_w", F.min("weight").over(window_spec)) \
            .withColumn("max_w", F.max("weight").over(window_spec)) \
            .withColumn("min_cat", F.min_by("category", "weight").over(window_spec)) \
            .withColumn("max_cat", F.max_by("category", "weight").over(window_spec))
    df.show(10, False)
    
    for r in [0.01, 0.99, 0.45, 0.75]:
        check_result(r, df)
