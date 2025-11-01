from itertools import accumulate
import json
import pytest
from pprint import pprint
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from random import random

from parrotpy import Parrot
from parrotpy.functions.core import rand_str, rand_num_str
from helpers.test_helpers import benchmark


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

def test_random_fk(spark):
    cust_n  = 100
    order_n = 10000

    customer_df = spark.range(cust_n).withColumnRenamed("id", "customer_id")
    order_df = spark.range(order_n).withColumnRenamed("id", "order_id")

    partition_n = 10
    # partitionBy("rand_key").
    w = Window.orderBy(F.monotonically_increasing_id())
    customer_df = (customer_df
        .withColumn("rand_key", F.floor(F.rand() * partition_n).cast("int"))
        .withColumn("cust_idx", (F.row_number().over(w)-1).cast("int"))
    )
    customer_df.count()

    order_df = (order_df
        .withColumn("rand_key", F.floor(F.rand() * partition_n).cast("int"))
        .withColumn("cust_idx", F.floor(F.rand() * cust_n).cast("int"))
    )

    join_cond = ["cust_idx"]
    joined_df = (order_df
        .join(F.broadcast(customer_df), on=join_cond, how="inner")
    )

    print("Joined:")
    joined_df.show(10, False)

    print("Order counts by customer:")
    joined_df.groupBy("customer_id").count() \
        .orderBy("customer_id") \
        .show(100, False)

    print("Total customers:")
    joined_df.groupBy("customer_id").count() \
        .agg(F.sum("count")) \
        .show(1, False)

def test_fast_row_number(spark):
    rows_count = 1_000_000
    partition_count = 10
    df = spark.range(rows_count)
    df.count()

    win = Window.orderBy(F.monotonically_increasing_id())
    df1 = df.withColumn("global_rn", F.row_number().over(win)-1)        
    print(df1.count())

    win = Window.partitionBy(F.spark_partition_id()).orderBy("id")
    df2 = (df
        .repartition(partition_count)
        .withColumn("partition_id", F.spark_partition_id())
        .withColumn("partition_rn", F.row_number().over(win)-1)
    )

    agg_df = (df2
        .groupBy("partition_id")
        .count()
        .withColumn("offset", F.sum("count").over(
            Window.orderBy("partition_id")) - F.col("count"))
    )

    final_df = (df2
        .join(F.broadcast(agg_df), on=["partition_id"])
        .withColumn("global_rn", F.col("partition_rn") + F.col("offset"))
        .select("id", "global_rn")
    )
    
    print(final_df.count())

 
def test_sql(spark):
    df = spark.range(3)

    c1 = F.col("id") + 10
    df1 = df.withColumn("id_plus_10", c1)
    df1.show(3, False)
    e1 = c1._jc.toString()
    print(e1)
    # df.withColumn("c1", F.expr(e1)).show(3, False)

    df2 = df.withColumn("id_plus_10", F.expr("id + 10"))
    df2.show(3, False)
    e2 = F.expr("id + 10").alias("c2")._jc.toString()
    print(e2)
    df2.selectExpr(e2).show(3, False) 

    c3 = F.rand(123)
    print(dir(c3._jc))

