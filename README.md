<img src="docs/images/parrotpy_logo.png" width="300" height="300" />



# ParrotPy

ParrotPy is a test/synthetic data generation tool for [Apache Spark](https://github.com/apache/spark).

**ALPHA stage, everything is subject to changed.**

# Flows

* Build test data from scratch.
* Or analyze existing DF to generate a json config file, and generate test data from it.
* For more customization, generate python code from json file and make tweaks, before generating test data.

# Usage

## Build from Scratch
```python
from parrotpy import Parrot
from pyspark.sql import functions as F
from parrotpy import functions as PF

parrot = Parrot(seed=123)

df = (parrot
  .df_builder()
  .options(name="employees")
  .build_column("id",               "int", PF.auto_increment(start=10000, step=3))
  .build_column("name",             "str", PF.common.person_name())
  .build_column("address",          "str", PF.common.address())
  .build_column("birth_year",       "int", PF.stats.uniform(min=1925, max=2025))
  .build_column("salary",           "int", PF.stats.normal(mean=70000, std_dev=10000))
  .build_column("office",        "string", PF.choices(["NY", "OH", "CA"], [0.5, 0.3, 0.2]))
  .build_column("start_date",      "date", PF.date_between("2000-01-01", "2025-12-31"))
  .build_column("last_login", "timestamp", PF.timestamp_between("2025-11-14 00:00:00", 
                                                                "2025-11-15 00:00:00"))
  .build_column("license_plate", "string", PF.regex_str(F.lit(r"[A-Z]{3}-[0-9]{4}")))
  .generate(1000)
)
```

Foreign Key
```python
customers_df = (
    parrot.df_builder()
    .options(name="customers")
    .build_column("cust_id", "int", PF.auto_increment(start=100))
    .build_column("name", "string", PF.common.person_name())
    .generate(20)
)

orders_df = (
    parrot.df_builder()
    .options(name="orders")
    .build_column("order_id", "int", PF.auto_increment(start=1000))
    .build_column("buyer_id", "int", PF.fk_references("customers.cust_id"))
    .generate(1000)
)
```


## Analyzing Existing Data

```python
# analyze existing df to generate an analysis report
src_df = (
    parrot.df_builder()
    .build_column("uniform_nums", "double", PF.stats.uniform(min=0, max=100))
    .build_column("normal_nums",  "double", PF.stats.normal(mean=10, std_dev=2))
    .generate(1000)
)

df_spec = parrot.analyzer().analyze_df(src_df)

# optional step, convert to json.
json_str = json.dumps(df_spec.to_dict())

{'columns': [
  { 'name':        'uniform_nums',
    'data_type':   'double',
    'value':{
      'entity_type': 'dist.uniform',
      'min_value':   0.26,
      'max_value':   99.88
    }
  },
  { 'name':        'normal_nums',
    'data_type':   'double',
    'value': {
      'entity_type': 'dist.normal',
      'mean':        10.08,
      'std_dev':     2.044
    }
  }
]}

# convert analysis to code
from parrotpy.code_gen.column_code_gen import inferred2code
code = inferred2code(df_spec)
print(code)

# generated code
from parrotpy.functions.stats import normal
from parrotpy.functions.stats import uniform
from parrotpy import Parrot


def generate_synthetic_data(spark):
    parrot = Parrot(spark)
    builder = (
        parrot.df_builder()
        .options(name="df1")
        .build_column("uniform_nums", "double", uniform(min_value=0.26, max_value=99.88))
        .build_column("normal_nums", "double", normal(mean=10.08, std_dev=2.044))
    )
    n = 100
    print(f"Starting generating {n} rows ...")
    return builder.gen_df(n)
```

# System Design

<img src="docs/images/parrot_flows.png" />
