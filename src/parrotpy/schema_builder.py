from typing import Any

from parrotpy.generators.stats import normal
from .schema import DfSchema, ComputedColumn, Invocation, InvocationColumn

class SchemaBuilder:
    def __init__(self, parrot):
        self.parrot = parrot
        self.schema = DfSchema()

    def build_from_dict(self, name: str, dtype: str, kwargs: dict):
        gen_fn = kwargs.get("gen")
        if gen_fn:
            del kwargs["gen"]
        invk_res = normal(**kwargs)  # TODO, call fn by name
        col = InvocationColumn(name, dtype, invk_res)
        self.schema.add_column(col)

        return self
    
    def build_column(self, name: str, dtype: str, col_value:Any = None, **kwargs: dict):
        """Build a column based on the provided attributes.

        Args:
            name (str): Column name.
            dtype (str): Data type of the column.
            attrs (dict): Attributes for the generator.

        Returns:
            Column: Spark Column.
        """
        print(f"build_column() col: {col_value}, kw: {kwargs}")
        if col_value is not None:
            if type(col_value).__name__ == "Column":
                col = ComputedColumn(name, dtype, col_value)
                self.schema.add_column(col)
            elif type(col_value) is Invocation:
                col = InvocationColumn(name, dtype, col_value)
                self.schema.add_column(col)
        else:
            self.build_from_dict(name, dtype, kwargs)

        return self

    def gen_df(self, row_count: int):
        df = self.parrot.empty_df(row_count)
        
        for col in self.schema.columns:
            df = col.generate(df)

        return df