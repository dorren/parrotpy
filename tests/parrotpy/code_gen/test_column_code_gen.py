import pytest
import ast

from parrotpy.models import DfSpec, ColumnSpec
from parrotpy.inference.analyzer import InferredEntity
from parrotpy.code_gen.column_code_gen import inferred2code, inferred2df

@pytest.fixture
def inferred_df1() -> DfSpec:

    return (DfSpec()
        .add_column(ColumnSpec(
            "name", "string", InferredEntity(entity_type="person")))
        .add_column(ColumnSpec(
            "age",  "int",    InferredEntity(entity_type="dist.normal", n=1, mean=40, std_dev=20)))
        .add_column(ColumnSpec(
            "city", "string", InferredEntity(entity_type="choices", elements=["NYC", "London", "Paris"])))
    )

def test_inferred2code(inferred_df1):
    code = inferred2code(inferred_df1)
    print(code)

def test_inferred2df(spark, inferred_df1):
    df = inferred2df(spark, inferred_df1)
    assert(df.count()==100)
    df.show(10, False)
    
