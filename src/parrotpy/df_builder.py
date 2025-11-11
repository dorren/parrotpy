from typing import Any

from parrotpy.functions.stats import normal
from .df_spec import DfSpec, ComputedColumn, Snapshot, SnapshotColumn
from .utils import snapshot

class DfBuilder:
    def __init__(self, parrot):
        self.parrot = parrot
        self.df_spec = DfSpec()

    def build_from_dict(self, name: str, dtype: str, kwargs: dict):
        fn_map = self.parrot.fn_map
        fn = fn_map.match(kwargs)
        if fn:
            del kwargs["distribution"]
            fn_ss = snapshot(fn)(**kwargs)
            col = SnapshotColumn(name, dtype, fn_ss)
            self.df_spec.add_column(col)
        else:
            raise ValueError(f"Can find function for given attributes {kwargs}")

        # if "gen" in kwargs:
        #     del kwargs["gen"]
        #     fn_ss = snapshot(normal)(**kwargs)  # TODO, call fn by name
        #     col = SnapshotColumn(name, dtype, fn_ss)
        #     self.schema.add_column(col)

        return self
    
    def build_column(self, name: str, dtype: str, col_value:Any = None, **kwargs: dict):
        """Build a column based on the provided attributes.

        Args:
            name (str): Column name.
            dtype (str): Data type of the column.
            attrs (dict): Attributes for the value generating function.

        Returns:
            Column: Spark Column.
        """
        if col_value is not None:
            if type(col_value).__name__ == "Column":
                col = ComputedColumn(name, dtype, col_value)
                self.df_spec.add_column(col)
            elif type(col_value) is Snapshot:
                col = SnapshotColumn(name, dtype, col_value)
                self.df_spec.add_column(col)
        else:
            self.build_from_dict(name, dtype, kwargs)

        return self

    def gen_df(self, row_count: int):
        return self.parrot.gen_df(self.df_spec, row_count)