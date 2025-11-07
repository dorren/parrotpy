from ..utils import snapshot 
from parrotpy.functions import stats
    
@snapshot
def uniform(n: int = 1, min_value: float = 0.0, max_value: float=1.0, seed: int=None, to_int: bool=False):
    return stats.uniform(n, min_value, max_value, seed, to_int)

@snapshot
def normal(n=1, mean=0, stddev=1.0, seed=None, to_int=False):
    return stats.normal(n, mean, stddev, seed, to_int)