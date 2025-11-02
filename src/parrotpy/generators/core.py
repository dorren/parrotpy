import importlib

from parrotpy.generators.stats import uniform, normal

def fn_path(func: callable):
    return f"{func.__module__}.{func.__name__}"

def get_fn(fn_path: str):
    """ get function reference from path """
    mod_name, name = fn_path.rsplit(".", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, name)
    return fn

def dist_fn_mapping():
    """ map random variable distribution type to generator function name """
    {
        "uniform": fn_path(uniform),
        "normal":  fn_path(normal)
    }