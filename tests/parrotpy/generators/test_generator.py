
import pytest
from pprint import pprint

def test_uniform(parrot):
    pr = parrot
    row_count = 10
    
    pr = (pr
        .build_column("u1", "double", gen="uniform")
        .build_column("u2", "double", gen="uniform", min_value=0, max_value=1000)
    )

    df = pr.gen_df(row_count)
    df.show(10, False)
    assert set(df.columns) == {"u1", "u2"}
    assert df.count() == row_count

def test_normal(parrot):
    pr = parrot
    row_count = 3
    
    pr = (pr
        .build_column("n1", "double", gen="normal")
        .build_column("n2", "double", gen="normal", n=1, mean=100, stddev=5, seed=123)
    )

    df = pr.gen_df(row_count)
    df.show(5, False)
    assert set(df.columns) == {"n1", "n2"}
    assert df.count() == row_count
