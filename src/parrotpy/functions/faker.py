from faker import Faker
from functools import partial
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


@udf(returnType=StringType())
def faker_udf(fn_name: str, seed: int = None):
    faker = Faker()
    if seed is not None:
        faker.seed_instance(seed)

    try:
        return getattr(faker, fn_name)()
    except Exception as e:
        logging.error(f"Error calling Faker method '{fn_name}': {e}")
        return F.lit(None)

def spark_faker(faker_fn_name: str, seed: int = None):
    """Return a Faker generated value by function name.
    """
    
    def generate(df: DataFrame, context: dict) -> DataFrame:
        col_name = context.get("column_name")

        if seed is None:
            return df.withColumn(col_name, faker_udf(F.lit(faker_fn_name)))
        else:
            seed_col = f"_{col_name}_seed"

            return (df
                .withColumn(seed_col, F.monotonically_increasing_id() + seed) 
                .withColumn(col_name, faker_udf(F.lit(faker_fn_name), seed_col))
                .drop(seed_col)
            )

    return generate

faker = spark_faker


__all__ = ['faker']