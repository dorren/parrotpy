from pyspark.sql import Column, DataFrame
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F



def normal(mean: float=0.0, sd: float=1.0, seed:int=None, to_int:bool=False) -> Column:
    """Generate a spark column with sample value from a normal distribution.

    Args:
        mean (float): The mean of the normal distribution.
        sd (float): The standard deviation of the normal distribution.
        seed (int, optional): randomization seed.
        to_int (bool, optional): Whether to return integer value. Defaults to False.

    Returns:
        Column: Spark Column.
    """
    value = F.randn(seed) * sd + mean
    value = F.rint(value).cast(IntegerType()) if to_int else value

    return value

def normal_array(array_size: int, mean: float=0.0, sd: float=1.0, seed:int=None, to_int:bool=False) -> Column:
    """Generate a spark column with array of sample values from a normal distribution.

    Args:
        array_size (int): Size of the array.
        mean (float): The mean of the normal distribution.
        sd (float): The standard deviation of the normal distribution.
        seed (int, optional): randomization seed.
        to_int (bool, optional): Whether to return integer values. Defaults to False.

    Returns:
        Column: Spark Column.
    """
    seed_expr = lambda i: seed + i if seed is not None else None
    samples = [
        normal(mean, sd, seed_expr(i), to_int)
        for i in range(array_size)
    ]
    return F.array(*samples)