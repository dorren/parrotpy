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
def builder(parrot):
    return parrot.df_builder()

def test_polymorphism(builder):
    n = 5
    ( builder.build_column("u1", "double", distribution="uniform", seed=111)
        .build_column("u2", "double", PF.stats.uniform(seed=123))
        .gen_df(n)
        .show(n, False)
    )
    pprint(builder.df_spec.to_dict())

def test_wrap(builder):
    n = 3
    uniform_ss = snapshot(PF.stats.uniform)
    builder.build_column("u1", "array<double>", uniform_ss(n=3,seed=1))
    builder.gen_df(n).show(n, False)
    pprint(builder.df_spec.to_dict())

def test_wrap_inline(builder):
    n = 2
    builder.build_column("u1", "array<double>", snapshot(PF.stats.uniform)(n=3,seed=1))
    df = builder.gen_df(n)
    assert(df.count() == n)
