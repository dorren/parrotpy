import pytest
from pprint import pprint

from parrotpy import functions as PF
from parrotpy.utils import fn_path, get_fn, snapshot
from parrotpy.functions.stats import uniform, normal

def test_call_by_name(spark):
    path = fn_path(uniform)
    fn = get_fn(path)
    assert fn == uniform
    print(fn(n=3, min_value=10, max_value=100))

@pytest.fixture
def sb(parrot):
    return parrot.schema_builder()

def test_polymorphism(sb):
    n = 5
    ( sb.build_column("u1", "double", gen="uniform", seed=111)
        .build_column("u2", "double", PF.stats.uniform(seed=123))
        .gen_df(n)
        .show(n, False)
    )
    pprint(sb.schema.to_dict())

def test_wrap(sb):
    n = 3
    uniform_ss = snapshot(PF.stats.uniform)
    sb.build_column("u1", "array<double>", uniform_ss(n=3,seed=1))
    sb.gen_df(n).show(n, False)
    pprint(sb.schema.to_dict())

def test_wrap_inline(sb):
    n = 2
    sb.build_column("u1", "array<double>", snapshot(PF.stats.uniform)(n=3,seed=1))
    df = sb.gen_df(n)
    assert(df.count() == n)
