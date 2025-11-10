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

def _uniform_choice(elements: list, seed: int=None) -> Column:
    n = len(elements)

    elems = F.array([F.lit(e) for e in elements])
    rand_idx = (F.rand(seed) * n).cast(IntegerType())
    return elems[rand_idx]

def _weighted_choice(elements: list, weights: list, rand_col: str, seed: int=None) -> Column:
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
    print(weights)
    pairs = list(zip(elements, cum_weights))
    total_weight = cum_weights[-1]

    if total_weight > 1.001 or total_weight < 0.999:
        raise ValueError(f"Weights must sum to 1.0, got {total_weight}")
    
    initial_cond = F.when(F.col(rand_col) <= F.lit(pairs[0][1]), F.lit(pairs[0][0]))
    chained_cond = reduce(
        lambda acc, pair: acc.when(F.col(rand_col) <= F.lit(pair[1]), F.lit(pair[0])),
        pairs[1:],
        initial_cond
    )
    chained_cond = chained_cond.otherwise(F.lit(pairs[-1][0]))
    print(chained_cond._jc.toString())
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
        return _weighted_choice(elements, weights, seed)

def weighted_choice(df: DataFrame, output_col_name: str, elements: list, weights: list, seed: int=None) -> DataFrame:
    """Add a column with weighted random choice from given elements.

    Args:
        df (DataFrame): Input Spark DataFrame.
        elements (list): List of elements to choose from.
        weights (list): Weights for each element.
        seed (int, optional): randomization seed.

    Returns:
        DataFrame: Spark DataFrame with new column "weighted_choice".
    """
    rand_col = F.rand(seed)
    choice_col = _weighted_choice(elements, weights, rand_col, seed)
    return df.withColumn(output_col_name, choice_col)
    