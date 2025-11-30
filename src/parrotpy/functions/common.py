from . import faker

def person_name(seed: int = None):
    return faker("name", seed)

def address(seed: int = None):
    return faker("address", seed)