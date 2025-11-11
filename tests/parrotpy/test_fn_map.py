import pytest
from pyspark.sql import Column

from parrotpy.parrot import Parrot, EntityMap
from parrotpy.functions.stats import normal, uniform


def test_fn_map():
    fm = EntityMap()
    fm.register("dist.normal", normal)
    fm.register("dist.uniform", uniform)

    assert("dist.normal" in fm)