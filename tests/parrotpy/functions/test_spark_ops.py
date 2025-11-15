from itertools import accumulate
import json
import pytest
from pprint import pprint
from pyspark.sql.types import DateType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from random import random

from parrotpy import Parrot
from helpers.test_helpers import benchmark


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




