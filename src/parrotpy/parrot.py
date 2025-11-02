from collections import UserDict
from pyspark.sql import SparkSession

from .schema import DfSchema
from parrotpy.generators.stats import Uniform, Normal

class Context(UserDict):
    def register(name: str, fn:callable):
        super().__setitem__(name, fn)


class SchemaBuilder:
    def __init__(self, parrot):
        self.parrot = parrot
        self.schema = DfSchema()
    
    def build_column(self, name: str, dtype: str, **kwargs: dict):
        """Build a column based on the provided attributes.

        Args:
            name (str): Column name.
            dtype (str): Data type of the column.
            attrs (dict): Attributes for the generator.

        Returns:
            Column: Spark Column.
        """
        gen_type = kwargs.get("gen")
        if gen_type:
            del kwargs["gen"]

        if gen_type == "uniform":
            new_col = Uniform(name, dtype, **kwargs)
        elif gen_type == "normal":
            new_col = Normal(name, dtype, **kwargs)
        
        self.schema.add_column(new_col)
        return self


    def gen_df(self, row_count: int):
        df = self.parrot.empty_df(row_count)
        
        for col in self.schema.columns:
            df = col.generate(df)

        return df


class Parrot:

    def __init__(self, spark: SparkSession):
        self.spark = spark

        # lib_name = __package__  # parrotpy
        # modules = ["common", "stats"]
        # for mod in modules:
        #     mod_name = f"{lib_name}.{mod}"
        #     setattr(self, mod, __import__(mod_name, fromlist=[""]))


    def empty_df(self, n: int):
        """Create an empty dataframe with n rows.

        Args:
            n (int): Number of rows.

        Returns:
            DataFrame: Spark DataFrame.
        """
        df = self.spark.range(n).drop("id")
        return df
    
    def schema_builder(self):
        return SchemaBuilder(self)
    


