from contextlib import contextmanager
import pytest
import timeit

from parrotpy import Parrot
from helpers.spark_helpers import spark

@contextmanager
def benchmark(name: str):
    start = timeit.default_timer()
    yield
    end = timeit.default_timer()
    print(f"[BENCHMARK] {name}: {end - start:.4f} seconds")

@pytest.fixture
def parrot(spark):
    pr = Parrot(spark)
    return pr

__all__ = ["benchmark", "parrot"]