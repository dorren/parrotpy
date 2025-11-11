import pytest
import ast, inspect
import textwrap
import black

from parrotpy import Parrot
from parrotpy.functions.stats import choices

@pytest.fixture
def src_code():
    return textwrap.dedent("""
        from parrotpy import Parrot
        from parrotpy import functions as PF

        parrot = Parrot(seed=123)
        builder = (parrot.df_builder()
            .build_column("id",         "int", PF.auto_increment(start=10000, step=3))
            .build_column("name",       "str", PF.name())
            .build_column("address",    "str", PF.address())
            .build_column("birth_year", "int", PF.stats.uniform(min=1925, max=2025))
            .build_column("salary",     "int", PF.stats.normal(mean=70000, std_dev=10000))
        )

        df = builder.generate(n=100)
    """)

# class BuildColumnCalls(ast.NodeTransformer):


def test_parse_code(src_code):
    tree = ast.parse(src_code)
    ast.fix_missing_locations(tree)
    new_code = ast.unparse(tree)

    # prettify with black formatter
    mode = black.Mode(target_versions={black.TargetVersion.PY311}, line_length=80)
    fmt_code = black.format_str(new_code, mode=mode)
    print(fmt_code)

def test_fn():
    params = {
        "name": "state",
        "data_type": "str"
    }
    fn_ref = choices
    fn_args = {"elements": ["NY", "CA", "TX"]}

    names = " ".join(list(params.keys()))
