import pytest
import ast

from parrotpy.df_spec import DfSpec, ColumnSpec
from parrotpy.code_gen.column_code_gen import spec2code, spec2df

@pytest.fixture
def df_spec1() -> DfSpec:

    return (DfSpec()
        .add_column(ColumnSpec("name", "string", "person name"))
        .add_column(ColumnSpec("age",  "int",    "dist.normal", **{"n":1, "mean": 40, "std_dev": 20}))
        .add_column(ColumnSpec("city", "string", "choices", **{"elements": ["NYC", "London", "Paris"]}))
    )

def test_spec2code(df_spec1):
    code = spec2code(df_spec1)
    print(code)

def test_spec2df(spark, df_spec1):
    df = spec2df(spark, df_spec1)
    assert(df.count()==100)
    df.show(10, False)
    
