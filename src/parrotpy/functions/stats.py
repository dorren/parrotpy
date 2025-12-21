from functools import reduce, partial
from itertools import accumulate
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

from .core import rand_array, rand_elem_or_array

def uniform_1(min_value: float = 0.0, max_value: float=1.0, seed: int=None, to_int: bool=False) -> Column:
    """Generate a single sample value from a uniform distribution.
    """
    diff = max_value - min_value
    value = F.rand(seed) * diff + min_value
    value = value.cast(IntegerType()) if to_int else value
    return value

def uniform(n: int = 1, min_value: float = 0.0, max_value: float=1.0, seed: int=None, to_int: bool=False) -> Column:
    """ generate single or array of normal distribution sample values
    """
    fn = partial(uniform_1, min_value=min_value, max_value=max_value, to_int=to_int)
    col_val = rand_elem_or_array(n, fn, seed)
    return col_val

def normal_1(mean: float=0.0, stddev: float=1.0, seed:int=None, to_int:bool=False) -> Column:
    """Generate a single sample value from a normal distribution.

    Args:
        mean (float): The mean of the normal distribution.
        stddev (float): The standard deviation of the normal distribution.
        seed (int, optional): randomization seed.
        to_int (bool, optional): Whether to return integer value. Defaults to False.

    Returns:
        Column: Spark Column.
    """
    value = F.randn(seed) * stddev + mean
    value = F.rint(value).cast(IntegerType()) if to_int else value

    return value

def normal(n: int = 1, mean: float = 0.0, std_dev: float = 1.0, seed: int = None, to_int: bool = False) -> Column:
    """ generate single or array of normal distribution sample values
    """
    fn = partial(normal_1, mean=mean, stddev=std_dev, to_int=to_int)
    col_val = rand_elem_or_array(n, fn, seed)
    return col_val

def mixed_normal(params_list, seed: int = None):
    """
    Creates a DataFrame with a mixture of normal distributions.
    
    :param params_list: List of dicts e.g., [{'mean': 10, 'stddev': 2, 'prob': 0.3}, ...]
    """
    def _mixed_normal(params_list, rand_col_name, seed): 
        cumulative_prob = 0.0
        mixture_expression = F.lit(None)
        z = F.randn(seed)

        for p in params_list:
            lower_bound = cumulative_prob
            upper_bound = cumulative_prob + p['prob']
            
            component_val = F.lit(p['mean']) + (F.lit(p['stddev']) * z)
            
            mixture_expression = F.when(
                (F.col(rand_col_name) >= lower_bound) & (F.col(rand_col_name) < upper_bound),
                component_val
            ).otherwise(mixture_expression)
            
            cumulative_prob = upper_bound

        return mixture_expression

    def generate(df: DataFrame, context: dict) -> DataFrame:
        col_name = context.get("column_name")
        rand_col_name = f"_{col_name}_rand"
        result_col = _mixed_normal(params_list, rand_col_name, seed)
        
        return (df
            .withColumn(rand_col_name, F.rand(seed)) 
            .withColumn(col_name, result_col)
            .drop(rand_col_name)
        )

    return generate
    