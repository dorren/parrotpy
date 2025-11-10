import pytest
import numpy as np
from fitter import Fitter
from fitter import Fitter, get_common_distributions
from pprint import pprint

from parrotpy.functions.stats import normal, uniform
from parrotpy.analyzer import Analyzer


@pytest.fixture
def nums_df(parrot):
    n = 1000
    builder = (parrot.df_builder()
        .build_column("u_nums", "double", uniform(n=1, min_value=0, max_value=100))
        .build_column("n_nums", "double", normal(n=1, mean=10, std_dev=2))
    )
    df = builder.gen_df(n)
    return df

def test_ks_test():
    dists = ["norm", "uniform"]
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

def test_distribution(parrot, nums_df):
    df = nums_df
    anlz = parrot.analyzer()

    result = anlz.analyze_numeric_column(df, "u_nums")
    assert(result["distribution"] == "uniform")
    
    result = anlz.analyze_numeric_column(df, "n_nums")
    assert(result["distribution"] == "norm")

def test_analyze_df(parrot, nums_df):
    schema = parrot.analyzer().analyze_df(nums_df)
    pprint(schema.to_dict())

    n = 5
    df = parrot.gen_df(schema, n)
    df.show(n, False)
    