from pyspark.sql import SparkSession

from parrotpy.functions import core
from parrotpy.generators.stats import Uniform, Normal

class Parrot:
    id_col_name = "_id"

    def __init__(self, spark: SparkSession):
        self.columns = []
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
        return core.empty_df(self.spark, n)
    
    def auto_increment(self, start: int = 0, step: int = 1):
        """Generate an auto-incrementing column starting from `start` with a given `step`.

        Args:
            start (int, optional): Starting value. Defaults to 0.
        """
        return core.auto_increment(start, step)


    def gen_df(self, row_count: int):
        df = self.empty_df(row_count)
        
        for col in self.columns:
            df = col.generate(df)

        return df
    
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

        if gen_type == "uniform":
            del kwargs["gen"]
            new_col = Uniform(name, dtype, **kwargs)
            self.columns.append(new_col)
        elif gen_type == "normal":
            del kwargs["gen"]
            new_col = Normal(name, dtype, **kwargs)
            self.columns.append(new_col)

        return self

    def schema(self):
        return [col.to_dict() for col in self.columns]
