from itertools import accumulate
import json
import pytest
from pprint import pprint
from pyspark.sql import Column
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from random import random

from parrotpy import Parrot
from helpers.test_helpers import benchmark

@pytest.fixture
def customers_df(spark):
    return spark.range(1000).withColumnRenamed("id", "customer_id")

@pytest.fixture
def orders_df(spark):
    return spark.range(1000).withColumnRenamed("id", "order_id")

def test_random_fk(spark, customers_df, orders_df):
    cust_n = 1000
    partition_n = 10

    # partitionBy("rand_key").
    w = Window.orderBy(F.monotonically_increasing_id())
    customers_df = (customers_df
        .withColumn("rand_key", F.floor(F.rand() * partition_n).cast("int"))
        .withColumn("cust_idx", (F.row_number().over(w)-1).cast("int"))
    )
    customers_df.count()

    orders_df = (orders_df
        .withColumn("rand_key", F.floor(F.rand() * partition_n).cast("int"))
        .withColumn("cust_idx", F.floor(F.rand() * cust_n).cast("int"))
    )

    join_cond = ["cust_idx"]
    joined_df = (orders_df
        .join(F.broadcast(customers_df), on=join_cond, how="inner")
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

def test_random_join(spark):
    ref_df_count =  20
    df_count     = 500
    ref_df = spark.range(ref_df_count).withColumnRenamed("id", "ref_id")
    df = spark.range(df_count)
    ref_df.count()
    df.count()

    index_col = "idx"
    fk_col  = "ref_id"
    new_col = "ref_id2"
    win = Window.orderBy(F.monotonically_increasing_id())
    ref_df = ref_df.withColumn(index_col, F.row_number().over(win)-1)        

    n = ref_df.count()
    df = (df.withColumn(index_col, F.floor(F.rand() * n).cast("int"))
        .join(F.broadcast(ref_df), on=[index_col], how="inner")
        .drop(index_col)
        .withColumnRenamed(fk_col, new_col)
    )

    # df.groupBy("ref_id").count().show(20, False)
    df.show(20, False)

def test_fast_row_number(spark):
    rows_count = 1_000_000
    partition_count = 10
    df = spark.range(rows_count)
    df.count()

    with benchmark("row_number, no partition"):
        win = Window.orderBy(F.monotonically_increasing_id())
        df1 = df.withColumn("global_rn", F.row_number().over(win)-1)        
        print(df1.count())

    with benchmark(f"row_number, {partition_count} partition"):
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
        
def test_types(spark):
    col = F.lit(None)
    print(isinstance(col, Column))

def test_mono_id(spark):
    n = 10
    df = spark.range(n).withColumn("mono_id", F.monotonically_increasing_id() + 1)
    df.show(n, False)




