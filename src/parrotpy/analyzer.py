from collections import Counter
import logging
import numpy as np
from pyspark.sql import DataFrame
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

    def __init__(self):
        pass

    def analyze(self, 
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
