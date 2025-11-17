import pytest
from pyspark.sql import Column

from parrotpy.inference.entity_map import EntityType, EntityMap
from parrotpy.functions.stats import normal, uniform


def test_entity_map():
    fm = EntityMap().default()
    assert(EntityType.DIST_NORMAL.value in fm)