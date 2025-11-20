from collections import Counter, UserDict
import logging
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField
from fitter import Fitter
from typing import List

from .entity_map import EntityType, EntityMap
from ..models import DfSpec, ColumnSpec, ColumnValue
from .. import functions as PF
    
class InferredEntity(UserDict, ColumnValue):
    def to_dict(self):
        return self


class NLPSingleton(object):
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(NLPSingleton, cls).__new__(cls)
        return cls.instance
    
    def __init__(self):
        import spacy
        self.nlp = spacy.load("en_core_web_sm")

    def categorize(self, texts: list):
        """ use NLP to categorize string into PERSON, ADDRESS, ORG, etc """
        acc = []
        for txt in texts:
            doc = self.nlp(txt)
            # print("Entities found using spaCy:")
            for ent in doc.ents:
                acc.append(ent.label_)
                # print(f"Text: {ent.text}, Label: {ent.label_}")            

        counter = Counter(acc)
        return counter.most_common(1)[0][0]
    
class Analyzer:
    default_sample_size = 200
    default_distributions = ["norm", "uniform"]

    def __init__(self, parrot):
        self.parrot = parrot


    def infer_distribution(self, nums: list, distributions: List[str]):
        """ get best matched random distribution name and attributes 
        """
        ettt = "entity_type"

        f = Fitter(nums, distributions=distributions)
        f.fit()
        dist_name = list(f.get_best())[0]

        if dist_name == "norm":
            mean = np.mean(nums).item()
            sd = np.std(nums, ddof=1).item()
            return InferredEntity(
                entity_type = EntityType.DIST_NORMAL.value,
                mean = round(mean, 3),
                std_dev = round(sd, 3)
            )
        elif dist_name == "uniform":
            return InferredEntity(
                entity_type = EntityType.DIST_UNIFORM.value,
                min_value = round(min(nums), 3),
                max_value = round(max(nums), 3)
            )
        
    def categorize_text(self, df: DataFrame, col_name: str) -> InferredEntity:
        texts = df.select(col_name).rdd.map(lambda row: row[0]).collect()
        category = NLPSingleton().categorize(texts)
        category = category.lower()
        return InferredEntity(entity_type=category)

    def analyze_numeric_column(self, 
                df: DataFrame, 
                col_name: str,
                sample_size:    int = default_sample_size, 
                distributions: list = default_distributions):
        total = df.count()
        sample_size = min(total, sample_size)
        logging.info(f"sample size for {col_name}: {sample_size}")

        fraction = 1.0 * sample_size / total
        df = df.select(col_name).sample(fraction=fraction)
        nums = df.rdd.map(lambda row: row[0]).collect()
        dist_attrs = self.infer_distribution(nums, distributions)
        return dist_attrs

    def is_numeric(self, data_type: str):
        num_types = ["int", "double"] #, "array<int>", "array<double>"]
        return data_type in num_types

    def analyze_df(self, df: DataFrame) -> DfSpec:
        df_spec = DfSpec()
        df.cache()

        for field in df.schema.fields:
            col_name = field.name
            data_type = field.dataType.simpleString()
            col_nullable = field.nullable

            if data_type == "string":
                inferred_entity = self.categorize_text(df, col_name)
                col_spec = ColumnSpec(col_name, data_type, inferred_entity)
                df_spec.add_column(col_spec)
            elif self.is_numeric(data_type):
                inferred_entity = self.analyze_numeric_column(df, col_name)
                col_spec = ColumnSpec(col_name, data_type, inferred_entity)
                df_spec.add_column(col_spec)
            else:
                logging.info(f"{data_type} unimplemented")

        df.unpersist()

        return df_spec

        