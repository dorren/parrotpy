import pytest
from pyspark.sql import Column

from parrotpy.parrot import Parrot, FunctionMap
from parrotpy.functions.stats import normal, uniform


def test_fn_map():
    fm = FunctionMap()
    fm.register("distribution.norm", normal)
    fm.register("distribution.uniform", uniform)

    assert("distribution.norm" in fm)