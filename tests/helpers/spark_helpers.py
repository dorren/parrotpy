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

__all__ = ["spark"]