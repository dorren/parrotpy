<img src="docs/images/parrotpy_logo.png" width="300" height="300" />

# ParrotPy

ParrotPy is a test/synthetic data generation tool for [Apache Spark](https://github.com/apache/spark).

# Usage

## Basic Usage
```Python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from parrotpy import Parrot
from parrotpy.stats import normal, add_random_array

pr = Parrot(
  seed=123,
  sample_size=1000
)

# create a new dataframe with id column, values from 0-9
df = spark.range(10)

# create a new column with value in normal distribution
# with mean=10, and standard deviation = 2.
new_col = normal(10, 2, to_int=False, seed=42)
df = df.withColumn("num", new_col)

# create new column with an array of 3 random values 
df = df.transform(add_random_array, "num_arr", new_col, 3)
```

## Replicate Production Data
```Python
# mimic an existing df
df = pr.mimic_df(src_df, spark=spark, n=100)

# For security or compliance reasons, one may not be allowed to generate test data 
# from production data in the same environment. In such case, you can extract 
# the meatadata and save as json file first, then review and modify as needed.

# mimic in two steps
metadata = pr.inspect_df(df)    # python dict
df = pr.gen_df(metadata, spark=spark, n=100) # gen 100 rows
```

## Add Custom Generators
```python

def category():
  choices = ["auto", "book", "electronics", "game", "household", "medical", "tool", "toy"]
  n = len(choices)
  weights = [1/n]*n
  return (choices, weight)

Parrot.register(
  namespace = "acme",
  function = category
)

pr = Parrot()
pr.acme.category()
```

## Configurations

Demographics
```json
{ "metadata": {
    "seed": 123
  }
  "columns": [
    {
      "name": "customer_name",
      "type": "string",
      "nullable": true,
      "metadata":{
        "generator":"common.name"
      }
    },
    {
      "name": "address",
      "type": "string",
      "nullable": true,
      "metadata":{
        "generator":"common.address"
      }
    }
  ]    
}
```

Column with categorical data
```json
{ "metadata": {
    "seed": 123
  }
  "columns": [
    {
      "name": "location",
      "type": "string",
      "nullable": true,
      "metadata":{
        "choices":["NY", "CA", "OH"],
        "weights":[0.3,  0.3,  0.4],
        "seed": 1234
      }
    }
  ]    
}
```

Column with probability distribution values
```json
{ "metadata": {
    "seed": 123
  }
  "columns": [
    {
      "name": "score",
      "type": "double",
      "nullable": true,
      "metadata": {
        "distribution": "norm",
        "mean": 0,
        "std_dev": 1,
        "seed": 1234
      }
    },
    {
      "name": "scores",
      "type":{
        "containsNull":false,
        "elementType":"double",
        "type":"array"
      },
      "nullable": false,
      "metadata": {
        "distribution": "norm",
        "mean": 0,
        "std_dev": 1,
        "seed": 1234, 
        "array_size": 3
      }
    }
  ]    
}
```

