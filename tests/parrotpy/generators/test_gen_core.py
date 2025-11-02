import pytest

from parrotpy.generators.core import fn_path, get_fn
from parrotpy.generators.stats import uniform, normal

def test_call_by_name(spark):
    path = fn_path(uniform)
    fn = get_fn(path)
    assert fn == uniform
    print(fn(n=3, min_value=10, max_value=100))