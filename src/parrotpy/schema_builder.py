from typing import Any

from parrotpy.functions.stats import normal
from .schema import DfSchema, ComputedColumn, Snapshot, SnapshotColumn
from .utils import snapshot

class SchemaBuilder:
    def __init__(self, parrot):
        self.parrot = parrot
        self.schema = DfSchema()

    def build_from_dict(self, name: str, dtype: str, kwargs: dict):
        gen_fn = kwargs.get("gen")
        if gen_fn:
            del kwargs["gen"]
        fn_ss = snapshot(normal)(**kwargs)  # TODO, call fn by name
        col = SnapshotColumn(name, dtype, fn_ss)
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
        if col_value is not None:
            if type(col_value).__name__ == "Column":
                col = ComputedColumn(name, dtype, col_value)
                self.schema.add_column(col)
            elif type(col_value) is Snapshot:
                col = SnapshotColumn(name, dtype, col_value)
                self.schema.add_column(col)
        else:
            self.build_from_dict(name, dtype, kwargs)

        return self

    def gen_df(self, row_count: int):
        df = self.parrot.empty_df(row_count)
        
        for col in self.schema.columns:
            df = col.generate(df)

        return df