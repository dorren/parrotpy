![Parrot logo](docs/images/parrot_white_bg.png)

# Test Data Parrot

ParrotPy is a test/synthetic data generation tool for Spark.

# Usage


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
df = df.withColumn(new_col_name, new_col)

# create new column with an array of 3 random values 
new_col = normal(mean=10, sd=2, seed=42)
df = df.transform(add_random_array, col_name, new_col, 3)

# mimic an existing df
df = pr.mimic_df(src_df, spark=spark, n=100)

# mimic in two steps
metadata = pr.inspect_df(df)
df = pr.gen_df(metadata, spark=spark, n=100) # gen 100 rows

```

## Configurations
column with categorical data
```json
{ metadata:
    seed: 123
  columns: [
    {"name": "location",
     "type": str,
     "choices":["NY", "CA", "OH"],
     "weights":[0.3,  0.3,  0.4],
     "seed": 1234
    }
  ]    
}
```

column with probability distribution values
```json
{ metadata:
    seed: 123
  columns: [
    {"name": "scores",
     "type": double,
     "distribution": "norm",
     "mean": 0,
     "std_dev": 1,
     "seed": 1234
    }
  ]    
}
```

