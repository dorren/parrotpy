import pytest
import ast

from parrotpy.df_spec import DfSpec, ColumnSpec
from parrotpy.code_gen.column_code_gen import dfspec2code

@pytest.fixture
def df_spec1() -> DfSpec:
    return (DfSpec()
        .add_column(ColumnSpec("name", "str", "person name"))
        .add_column(ColumnSpec("age",  "int", "dist.uniform", **{"mean": 30, "std_dev": 20}))
        .add_column(ColumnSpec("city", "str", "choices", **{"elements": ["NYC", "London", "Paris"]}))
    )

def test_transform(df_spec1):
    code = dfspec2code(df_spec1)
    print(code)
