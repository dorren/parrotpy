import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    spark = (SparkSession.builder
            .master("local[*]")
            .appName("ParrotPy-Test") 
            .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
            .config("spark.python.worker.faulthandler.enabled", "true")
            .getOrCreate())
    spark.sparkContext.setLogLevel("INFO")

    return spark

def assert_df_equal(df1, df2):
    """Assert that two Spark DataFrames are equal."""
    assert df1.schema == df2.schema, "Schemas do not match"
    assert df1.subtract(df2).union(df2.subtract(df1)).count() == 0, "DataFrames content do not match"
    
__all__ = ["spark"]
