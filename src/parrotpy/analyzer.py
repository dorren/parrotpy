from collections import Counter
import logging
import numpy as np
from pyspark.sql import DataFrame
from pyspark.sql.types import StructField
from fitter import Fitter
from typing import List


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
        f = Fitter(nums, distributions=distributions)
        f.fit()
        dist_name = list(f.get_best())[0]

        if dist_name == "norm":
            mean = np.mean(nums).item()
            sd = np.std(nums, ddof=1).item()
            return {
                "distribution": dist_name,
                "mean": round(mean, 3),
                "std_dev": round(sd, 3)
            }
        elif dist_name == "uniform":
            return {
                "distribution": dist_name,
                "min_value": round(min(nums), 3),
                "max_value": round(max(nums), 3)
            }
        
    def categorize_text(self, words: list):
        pass

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
        num_types = ["int", "double", "array<int>", "array<double>"]
        return data_type in num_types

    def analyze_df(self, df: DataFrame):
        sb = self.parrot.schema_builder()

        for field in df.schema.fields:
            col_name = field.name
            data_type = field.dataType.simpleString()
            col_nullable = field.nullable

            if self.is_numeric(data_type):
                dist_attrs = self.analyze_numeric_column(df, col_name)
                sb.build_column(col_name, data_type, **dist_attrs)
            else:
                logging.info("unimplemented")

        return sb.schema

        