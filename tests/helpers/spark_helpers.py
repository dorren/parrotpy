import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # py_exe = "C:/Users/dorren/AppData/Local/pypoetry/Cache/virtualenvs/parrotpy-_LyJFcJ7-py3.11/Scripts/python.exe"
    """Create a Spark session for testing."""
    spark = (SparkSession.builder    \
            .master("local[*]")      \
            .appName("ParrotPy-Test") \
            .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true") \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .getOrCreate())
    spark.sparkContext.setLogLevel("INFO")

    return spark