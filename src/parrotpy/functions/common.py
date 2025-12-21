from . import faker, faker_array

def person_name(seed: int = None):
    return faker("name", seed)

def person_names(size: int, seed: int = None):
    return faker_array(size, "name", seed)

def address(seed: int = None):
    return faker("address", seed)