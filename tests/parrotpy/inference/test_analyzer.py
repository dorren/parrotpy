import pytest
import numpy as np
from fitter import Fitter
from fitter import Fitter, get_common_distributions
from pprint import pprint

from parrotpy import functions as PF
from parrotpy.functions.stats import normal, uniform
from parrotpy.inference.entity_map import EntityType
from parrotpy.inference.analyzer import Analyzer, InferredEntity
from parrotpy.code_gen.column_code_gen import inferred2code


@pytest.fixture
def sample_df(parrot):
    n = 1000
    builder = (parrot.df_builder()
        .options(name="nums_df")
        .build_column("name", "string", PF.common.person_name())
        .build_column("u_nums", "double", uniform(n=1, min_value=0, max_value=100))
        .build_column("n_nums", "double", normal(n=1, mean=10, std_dev=2))
    )
    df = builder.generate(n)
    return df

@pytest.mark.skip(reason="too slow")
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

@pytest.mark.skip(reason="too slow")
def test_distribution(parrot, sample_df):
    df = sample_df
    anlz = parrot.analyzer()

    result = anlz.analyze_numeric_column(df, "u_nums")
    assert(result["entity_type"] == EntityType.DIST_UNIFORM.value)
    
    result = anlz.analyze_numeric_column(df, "n_nums")
    print(result)
    assert(result["entity_type"] == EntityType.DIST_NORMAL.value)

def test_analyze_df(parrot, sample_df):
    df_spec = parrot.analyzer().analyze_df(sample_df)

    pprint(df_spec.to_dict())

    code = inferred2code(df_spec)
    print(code)