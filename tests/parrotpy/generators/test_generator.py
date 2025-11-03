
import pytest
from pprint import pprint

from parrotpy import functions as PF
from parrotpy import generators as PG
from parrotpy.schema import signatured

@pytest.fixture
def sb(parrot):
    ''' schema_builder() '''
    return parrot.schema_builder()

# def test_uniform(sb):
#     row_count = 5
#     sb.build_column("u1", "double", gen="uniform") \
#         .build_column("u2", "double", gen="uniform", min_value=0, max_value=1000)

#     df = sb.gen_df(row_count)
#     # df.show(10, False)
#     assert set(df.columns) == {"u1", "u2"}
#     assert df.count() == row_count

# def test_normal(sb):
#     row_count = 5
#     sb.build_column("n1", "double", gen="normal") \
#         .build_column("n2", "double", gen="normal", n=1, mean=100, stddev=5, seed=123)
    
#     df = sb.gen_df(row_count)
#     # df.show(5, False)
#     assert set(df.columns) == {"n1", "n2"}
#     assert df.count() == row_count

#     pprint(sb.schema.to_dict())

def test_polymorphism(sb):
    n = 5
    sb.build_column("u1", "double", gen="uniform", seed=111)
    sb.build_column("u2", "double", PF.stats.uniform(seed=123))
    sb.build_column("u3", "double", PG.stats.uniform(min_value=100, max_value=1000))
    sb.gen_df(n).show(n, False)
    pprint(sb.schema.to_dict())

def test_wrap(sb):
    n = 3
    wrapped_fn = signatured(PF.stats.uniform)

    sb.build_column("u1", "double", wrapped_fn(seed=1))
    sb.gen_df(n).show(n, False)
    pprint(sb.schema.to_dict())

