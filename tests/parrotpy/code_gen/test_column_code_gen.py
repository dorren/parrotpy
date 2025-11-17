import pytest
import ast

from parrotpy.df_spec import DfSpec
from parrotpy.inference.analyzer import InferredDf, InferredColumn
from parrotpy.code_gen.column_code_gen import inferred2code, inferred2df

@pytest.fixture
def inferred_df1() -> DfSpec:

    return (InferredDf()
        .add_column(InferredColumn("name", "string", "person name"))
        .add_column(InferredColumn("age",  "int",    "dist.normal", **{"n":1, "mean": 40, "std_dev": 20}))
        .add_column(InferredColumn("city", "string", "choices", **{"elements": ["NYC", "London", "Paris"]}))
    )

def test_inferred2code(inferred_df1):
    code = inferred2code(inferred_df1)
    print(code)

def test_inferred2df(spark, inferred_df1):
    df = inferred2df(spark, inferred_df1)
    assert(df.count()==100)
    df.show(10, False)
    
