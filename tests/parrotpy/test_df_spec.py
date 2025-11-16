import pytest

def test_options(parrot):
    builder = parrot.df_builder()
    builder.options(name="df1", format="parquet", speed="fast")

    spec = builder.df_spec
    assert("speed" not in spec.spec_options)