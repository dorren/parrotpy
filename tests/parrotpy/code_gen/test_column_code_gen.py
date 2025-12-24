import pytest
import ast

from parrotpy.models import DfSpec, ColumnSpec
from parrotpy.inference.analyzer import InferredEntity
from parrotpy.inference.inferred_df_spec import InferredDfSpec

@pytest.fixture
def inferred_df_spec_1() -> DfSpec:

    return (InferredDfSpec()
        .add_column(ColumnSpec(
            "name", "string", InferredEntity(entity_type="person")))
        .add_column(ColumnSpec(
            "age",  "int",    InferredEntity(entity_type="dist.normal", n=1, mean=40, std_dev=20)))
        .add_column(ColumnSpec(
            "city", "string", InferredEntity(entity_type="choices", elements=["NYC", "London", "Paris"])))
    )

def test_inferred2code(inferred_df_spec_1):
    code = inferred_df_spec_1.to_code()
    print(code)

def test_inferred2df(spark, inferred_df_spec_1):
    df = inferred_df_spec_1.to_df(spark)
    assert(df.count()==100)
    df.show(10, False)
    
