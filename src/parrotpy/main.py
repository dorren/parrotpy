from pyspark.sql.types import StructField, StructType

from parrotpy.stats import normal


class Parrot:
    id_col_name = "_id"

    def __init__(self):
        lib_name = __package__  # parrotpy

        modules = ["common", "stats"]
        for mod in modules:
            mod_name = f"{lib_name}.{mod}"
            setattr(self, mod, __import__(mod_name, fromlist=[""]))


    def gen_df(self, spark, schema: StructType, row_count: int):
        df = spark.range(row_count) \
          .withColumnRenamed("id", "_id")
        
        for field in schema.fields:
            new_col = self.gen_field(field)
            df = df.withColumn(field.name, new_col.cast(field.dataType))

        df = df.drop(self.id_col_name)
        return df
    
    def gen_field(self, field_schema: StructField):
        md = field_schema.metadata

        if md.get("distribution") == "normal":
            return normal(md.get("mean", 0), md.get("stddev", 1), seed=md.get("seed", None))
        else:
            raise ValueError(f"Unsupported distribution: {md.get('distribution')}")