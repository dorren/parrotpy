import pytest
from pyspark.sql import Column

from parrotpy.parrot import Parrot
from parrotpy import functions as PF

def test_use_reference(parrot):
    customers_df = (parrot.df_builder()
        .options(name="customers")
        .build_column("cust_id", "int", PF.core.auto_increment(start=100))
        .build_column("name", "string", PF.common.person_name())
        .gen_df(20)
    )

    orders_df = (parrot.df_builder()
        .options(name="orders")
        .build_column("order_id",   "int", PF.core.auto_increment(start=1000))
        .build_column("fk_cust_id", "int", PF.core.fk_references("customers.cust_id"))
        .gen_df(1000)
    )

    assert(orders_df.count() == 1000)
