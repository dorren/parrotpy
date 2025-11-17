<img src="docs/images/parrotpy_logo.png" width="300" height="300" />

# ParrotPy

ParrotPy is a test/synthetic data generation tool for [Apache Spark](https://github.com/apache/spark).

# Flows
<img src="docs/images/parrot_flows.png" />

* Build test data from scratch.
* Or analyze existing DF to generate a json spec file, and generate test data from it.
* For more customization, generate python code from spec json file and make tweaks, before generating test data.

# Usage

## Build from Scratch
```python
from parrotpy import Parrot
from parrotpy import functions as PF

parrot = Parrot(seed=123)

df = (parrot
  .df_builder()
  .options(name="employees")
  .build_column("id",         "int", PF.auto_increment(start=10000, step=3))
  .build_column("name",       "str", PF.common.person_name())
  .build_column("address",    "str", PF.common.address())
  .build_column("birth_year", "int", PF.stats.uniform(min=1925, max=2025))
  .build_column("salary",     "int", PF.stats.normal(mean=70000, std_dev=10000, to_int=True))
  .build_column("office",  "string", PF.choices(["NY", "OH", "CA"], [0.5, 0.3, 0.2]))
  .build_column("start_date","date", PF.date_between("2000-01-01", "2025-12-31"))
  .build_column("last_login", "timestamp", PF.timestamp_between("2025-11-14 00:00:00", "2025-11-15 00:00:00"))
  .gen_df(1000)
)
```

Foreign Key
```python
parrot = Parrot()

customers_df = (
    parrot.df_builder()
    .options(name="customers")
    .build_column("cust_id", "int", PF.auto_increment(start=100))
    .build_column("name", "string", PF.common.person_name())
    .gen_df(20)
)

orders_df = (
    parrot.df_builder()
    .options(name="orders")
    .build_column("order_id", "int", PF.auto_increment(start=1000))
    .build_column("buyer_id", "int", PF.fk_references("customers.cust_id"))
    .gen_df(1000)
)
```


## From Analyzing Existing Data

```python
# analyze existing df to generate an analysis report
parrot = Parrot(seed=123)
df_analysis = parrot.analyzer().analyze_df(src_df)

# optional step, convert to json.
json_str = json.dumps(df_analysis.to_dict())

{'columns': [
  { 'name': 'uniform_nums',
    'data_type': 'double',

    'entity_type': 'dist.uniform',
    'min_value': 0.26,
    'max_value': 99.88
  },
  { 'name': 'normal_nums',
    'data_type': 'double',

    'entity_type': 'dist.normal',
    'mean': 10.08,
    'std_dev': 2.044
  }
]}

# convert analysis to code
from parrotpy.code_gen.column_code_gen import inferred2code
code = inferred2code(df_analysis)
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
        .build_column(
            "uniform_nums", "double", uniform(min_value=0.26, max_value=99.88)
        )
        .build_column("normal_nums", "double", normal(mean=10.08, std_dev=2.044))
    )
    n = 100
    print(f"Starting generating {n} rows ...")
    return builder.gen_df(n)
```
For security or compliance reasons, developer may not be allowed to generate test data in the production environment. In such case, you can generate the data analysis and save as json file first, then customize in dev environment as needed.


## Convert Df_spec to Python Code
```python
code_str = parrot.spec2code(df_spec)
```

In command shell
```shell
# analyze df and produce df_spec json file
parrot analyze customers.df   --output customers.json

# analyze df and produce python code
parrot analyze customers.df   --output customers.py

# convert df_spec json to python code
parrot json2py customers.json --output customers.py
```