from pyspark.sql import Column
from pyspark.sql import functions as F


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

__all__ = [
    "auto_increment",
    "rand_str", 
    "rand_num_str", 
    "rand_array", 
    "rand_elem_or_array",
    "date_between",
    "timestamp_between",
    "nothing"
]