from collections import UserDict
from enum import Enum
import logging
from parrotpy import functions as PF

class EntityType(Enum):
    PERSON = "person"
    CHOICES = "choices"
    DIST_NORMAL = "dist.normal"
    DIST_UNIFORM = "dist.uniform"
    UNKNOWN = "unknown"
    
class EntityMap(UserDict):

    """ map entity type to the value generating function. For example:

        entity type "person"       would be mapped to parrotpy.functions.common.person_name()
        entity type "dist.normal"  would be mapped to parrotpy.functions.stats.normal()
        entity type "dist.uniform" would be mapped to parrotpy.functions.stats.uniform()
    """
    def register(self, name: str, fn:callable):
        super().__setitem__(name, fn)

    def get(self, entity_type: str):
        """ find the right function for the entity type """
        fn = super().get(entity_type)

        if fn is None:
            logging.warning(f"no function found for entity_type {entity_type}")
            fn = PF.core.nothing()
            
        return fn


    @classmethod
    def default(cls):
        em = cls()
        em.register("unknown",      PF.nothing)
        em.register("person",       PF.common.person_name)
        em.register("address",      PF.common.address)
        em.register("choices",      PF.choices)
        em.register("dist.normal",  PF.stats.normal)
        em.register("dist.uniform", PF.stats.uniform)

        return em
