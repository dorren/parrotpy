import pytest
import numpy as np
from fitter import Fitter
from fitter import Fitter, get_common_distributions

from parrotpy.functions.stats import normal, uniform
from parrotpy.analyzer import Analyzer

def test_ks_test():
    dists = ["norm", "uniform", "expon"]
    # print(get_common_distributions())

    data = np.random.normal(size=100)
    f = Fitter(data, distributions=dists)
    f.fit()
    dist_name = list(f.get_best())[0]
    assert dist_name == "norm"

    data = np.random.uniform(size=100, low=50, high=100)
    f = Fitter(data, distributions=dists)
    f.fit()
    dist_name = list(f.get_best())[0]
    assert dist_name == "uniform"

def test_df(parrot):
    n = 1000
    sb = (parrot.schema_builder()
        .build_column("u_nums", "double", uniform(n=1, min_value=0, max_value=100))
        .build_column("n_nums", "double", normal(n=1, mean=10, stddev=2))
    )
    df = sb.gen_df(n)
    anlz = parrot.analyzer()

    result = anlz.analyze(df, "u_nums")
    assert(result["distribution"] == "uniform")
    
    result = anlz.analyze(df, "n_nums")
    assert(result["distribution"] == "norm")
