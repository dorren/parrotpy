<img src="docs/images/parrotpy_logo.png" width="300" height="300" />

# ParrotPy

ParrotPy is a test/synthetic data generation tool for [Apache Spark](https://github.com/apache/spark).

# Usage

## Basic Usage
```python
# brainstorm

from parrotpy import Parrot
from parrotpy.stats import uniform

pr = Parrot(
  seed=123,
  sample_size=1000
)

# row value can be generator function, spark function/expression.
cust_schema = (pr
  .build_column("id",         "int", pr.auto_increment(start=10000, step=3))
  .build_column("name",       "str", pr.common.name())
  .build_column("address",    "str", pr.common.address())
  .build_column("birth_year", "int", pr.stats.uniform(min=1925, max=2025))
  .build_column("salary",     "int", 
      pr.stats.normal(mean=70000, std_dev=10000)
        .with_nulls(prob=0.1)
  )
)

pr.generate(n=100, schema=cust_schema)
```



```Python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from parrotpy import Parrot
from parrotpy.stats import normal, add_random_array

pr = Parrot(
  spark = spark,
  seed=123,
  sample_size=1000
)

df = pr.empty_df()

df = df.withColumn("norm_num", pr.normal())

# create a new column with value in normal distribution
# with mean=10, and standard deviation = 2.
df.withColumn("norm2", pr.normal(10, 2, to_int=False, seed=42))

# create array column of size 3 with normal sample values.
df.withColumn("num_arr", pr.normal_array(3))
```

## Replicate Production Data
```Python
# mimic an existing df
df = pr.mimic_df(src_df)

# For security or compliance reasons, one may not be allowed to generate test data 
# from production data in the same environment. In such case, you can extract 
# the meatadata and save as json file first, then review and modify as needed.

# mimic in two steps
schema = pr.get_schema(df)    # get df schema with data statistics.
df = pr.gen_df(schema, n=100) # generate 100 rows
```

## Add Custom Generators
```python

def category():
  choices = ["auto", "books", "electronics", "game", "household", "medical", "tools", "toys"]
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

For column without statistical attributes, then generator has to be explicitly defined.
```json
{ 
  "seed": 123,
  "columns": [
    {
      "name": "customer_name",
      "type": "string",
      "nullable": true,
      "generator":"common.name"
    },
    {
      "name": "address",
      "type": "string",
      "nullable": true,
      "generators": [
        {
          "name":"common.address", 
          "args":[]
        },
        {
          "name":"common.with_nulls", 
          "args":[0.1]
        },
      ]
    }
  ]    
}
```

Column with categorical data
```json
{ 
  "seed": 123,
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
{ 
  "count": 100,
  "seed": 123
  "columns": [
    {
      "name": "score",
      "type": "double",
      "nullable": true,
      "distribution": "norm",
      "mean": 0,
      "std_dev": 1,
      "seed": 1234
    },
    {
      "name": "scores",
      "type":{
        "containsNull":false,
        "elementType":"double",
        "type":"array"
      },
      "nullable": false,
      "distribution": "norm",
      "mean": 0,
      "std_dev": 1,
      "seed": 1234, 
      "array_size": 3
    }
  ]    
}
```

Table with foreign keys. 

For example, customer_id is generated already. Pass the refrence, and it will use id column from dataframe stored in a dictionary of key "customers". 
```json
{ 
  "name": "orders",
  "columns": [
    {
      "name": "order_id",
      "type": "integer",
      "nullable": true,
      "auto_increment": true  
    },
    {
      "name": "customer_id",
      "type": "integer",
      "nullable": true,
      "references": "customers.id"
    }
  ]
}
```
auto_increment's value can be 
* true, starts at 1. 
* [1000], starts at 1000
* [1000, 10], starts at 1000, and increment by 10.


```python
pr = Parrot()

dfs = {}
dfs["customers"] = pr.gen("customers.json")
dfs["orders"]    = pr.gen("orders.json", dataframes=dfs)
```

