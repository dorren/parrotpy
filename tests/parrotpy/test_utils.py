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
    return parrot.df_builder().options(name="df1")

def test_wrap(builder):
    n = 3
    uniform_ss = snapshot(PF.stats.uniform)
    builder.build_column("u1", "array<double>", uniform_ss(n=3,seed=1))
    df = builder.gen_df(n)
    assert(df.count() == n)

def test_wrap_inline(builder):
    n = 2
    builder.build_column("u1", "array<double>", snapshot(PF.stats.uniform)(n=3,seed=1))
    df = builder.gen_df(n)
    assert(df.count() == n)
