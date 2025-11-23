from functools import reduce, partial
from itertools import accumulate
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import rstr

from ..utils import snapshot, Snapshot

def auto_increment(start: int = 0, step: int = 1) -> Column:
    """Generate an auto-incrementing column starting from `start` with a given `step`.

    Args:
        start (int, optional): Starting value. Defaults to 0.

    Returns:
        Dataframe: df with new auto-incremented Column.
    """
    return F.expr(f"{start} + (row_number() OVER (ORDER BY (SELECT NULL)) - 1) * {step}")

def rand_str(n, seed=None) -> Column:
    ''' 
    Generate <n> random character string. Backport F.randstr(n), which is only 
    available in Spark 4.0.0+.

    ```
    F.rand_str(3)        # "ABC"
    F.rand_str(5, 1000)  # "ABCDE", seed=1000
    ```

    '''
    seed_expr = lambda i: seed + i if seed is not None else None
    r_char    = lambda i: F.char(65 + (F.rand(seed_expr(i))*26).cast("int"))
    result = F.concat(*[r_char(i) for i in range(n)])

    return result

def rand_num_str(n, seed=None) -> Column:
    ''' 
    Generate <n> random numbers as string
    '''
    seed_expr = lambda i: seed + i if seed is not None else None
    r_num     = lambda i: (F.rand(seed_expr(i))*10).cast("int").cast("string")
    result = F.concat(*[r_num(i) for i in range(n)])

    return result

@udf(returnType=StringType())
def regex_str(pattern:str) -> Column:
    """ generate a random string by regex pattern. For example:

    regex_str("[A-Z]{3}-[0-9]{4}")    # 3 letter, 4 digits
    """
    return rstr.xeger(pattern)
    

def rand_array(n: int, gen_fn, seed=None) -> Column:
    ''' 
    Generate array of n random elements with given generator function.
    '''
    seed_expr = lambda i: seed + i if seed is not None else None
    arr = F.array(*[gen_fn(seed=seed_expr(i)) for i in range(n)])

    return arr

def rand_elem_or_array(n: int, gen_fn, seed=None) -> Column:
    if n == 1:
        return gen_fn(seed=seed)  
    else:
        return rand_array(n, gen_fn, seed)

def date_between(start_date_str:str, end_date_str:str) -> Column:
    """ generate random dates between the parameters. 
        date str shall be in "YYYY-MM-DD" format.
    """
    start_date = F.lit(start_date_str).cast("date")
    end_date = F.lit(end_date_str).cast("date")
    
    return F.date_add(start_date,
        (F.round(F.rand() * (F.date_diff(end_date, start_date)))).cast("int")
    )

def timestamp_between(start_ts_str:str, end_ts_str:str) -> Column:
    """ generate random dates between the parameters. 
        date str shall be in "YYYY-MM-DD HH:mm:ss" format.
    """
    start_ts = F.lit(start_ts_str).cast("timestamp")
    end_ts   = F.lit(end_ts_str).cast("timestamp")
    
    expr = F.from_unixtime(
        F.unix_timestamp(start_ts)
        + (F.rand() * (F.unix_timestamp(end_ts) - F.unix_timestamp(start_ts)))
    ).cast("timestamp")

    return expr

def nothing(**kwargs) -> Column:
    return F.lit(None)



def _fk_references(local_df: DataFrame, fk_df: DataFrame, fk_col_name:str, new_col_name: str=None) -> DataFrame:
    """ join reference_df, select <fk_col_name> value randomly, and rename it to <new_col_name>"""
    
    index_col = f"_{fk_col_name}_idx"
    new_col_name = new_col_name if new_col_name else fk_col_name

    win = Window.orderBy(F.monotonically_increasing_id())
    fk_df = fk_df.withColumn(index_col, F.row_number().over(win)-1)        

    n = fk_df.count()
    local_df = (local_df
        .alias("L")
        .withColumn(index_col, F.floor(F.rand() * n).cast("int"))
        .join(fk_df.alias("R"), on=[index_col], how="inner")
        .drop(index_col)
        .withColumnRenamed(fk_col_name, new_col_name)
    )
    local_df = local_df.select(F.col("L.*"), F.col(new_col_name))
    return local_df

def fk_references(fk_path: str):
    """ define a foreign key column. parameter shall be in the format of "<df_name>.<column_name>", 
        For example, "customers.cust_id". Referenced df should existed in parrot object. 
    """

    def generate(df: DataFrame, col_spec, context:dict) -> DataFrame:
        df_name, fk_col_name = fk_path.split(".")
        fk_df = context["dataframes"].get(df_name)
         
        df2 = _fk_references(df, fk_df, fk_col_name, col_spec.name)
        return df2

    return generate



def _uniform_choice(elements: list, seed: int=None) -> Column:
    n = len(elements)

    elems = F.array([F.lit(e) for e in elements])
    rand_idx = (F.rand(seed) * n).cast("int")
    return elems[rand_idx]

def _weighted_choice(elements: list, weights: list, rand_col_name: str, seed: int=None) -> Column:
    """Generate a spark column with weighted random choice from given elements.

    Args:
        elements (list): List of elements to choose from.
        weights (list): Weights for each element.
        rand_col (str): Name of the column with random values.
        seed (int, optional): randomization seed.

    Returns:
        Column: Spark Column.
    """
    cum_weights = list(accumulate(weights))
    pairs = list(zip(elements, cum_weights))
    total_weight = cum_weights[-1]

    if total_weight > 1.001 or total_weight < 0.999:
        raise ValueError(f"Weights must sum to 1.0, got {total_weight}")
    
    initial_cond = F.when(F.col(rand_col_name) <= F.lit(pairs[0][1]), F.lit(pairs[0][0]))
    chained_cond = reduce(
        lambda acc, pair: acc.when(F.col(rand_col_name) <= F.lit(pair[1]), F.lit(pair[0])),
        pairs[1:],
        initial_cond
    )
    chained_cond = chained_cond.otherwise(F.lit(pairs[-1][0]))
    return chained_cond

def choices(elements: list, weights: list=None, seed: int=None) -> Column:
    """Generate a spark column with random choice from given elements.

    Args:
        elements (list): List of elements to choose from.
        weights (list, optional): Weights for each element. Defaults to None.
        seed (int, optional): randomization seed.

    Returns:
        Column: Spark Column.
    """
    if weights is None:
        return _uniform_choice(elements, seed)
    else:
        return weighted_choices(elements, weights, seed)

        
def weighted_choices(elements: list, weights: list, seed: int=None) -> DataFrame:
    """Add a column with weighted random choice from given elements.

    Args:
        df (DataFrame): Input Spark DataFrame.
        elements (list): List of elements to choose from.
        weights (list): Weights for each element.
        seed (int, optional): randomization seed.

    Returns:
        DataFrame: Spark DataFrame with new column "weighted_choice".
    """

    def generate(df: DataFrame, col_spec, context) -> DataFrame:
        rand_col_name = f"_{col_spec.name}_rand"
        choice_col = _weighted_choice(elements, weights, rand_col_name, seed)
        
        return (df
            .withColumn(rand_col_name, F.rand(seed)) 
            .withColumn(col_spec.name, choice_col)
            .drop(rand_col_name)
        )

    return generate

__all__ = [
    "auto_increment",
    "rand_str", 
    "regex_str",
    "rand_num_str", 
    "rand_array", 
    "rand_elem_or_array",
    "date_between",
    "timestamp_between",
    "nothing",
    "_fk_references",
    "fk_references",
    "_uniform_choice",
    "_weighted_choice",
    "choices",
    "weighted_choices"
]