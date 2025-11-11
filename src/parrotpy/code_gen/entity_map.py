from collections import UserDict


class EntityMap(UserDict):
    """ map entity type to the value generating function. For example:

        entity type "person" would be mapped to parrotpy.functions.name()
        entity type "uniform distribution" would be mapped to parrotpy.functions.stats.uniform()
    """
    def register(self, name: str, fn:callable):
        super().__setitem__(name, fn)

    def match(self, analyze_result: dict):
        """ find the right function for the DF analyze result """
        if "distribution" in analyze_result:
            dist_type = analyze_result["distribution"]

            if dist_type == "norm":
                fn = self["normal distribution"]
            elif dist_type == "uniform":
                fn = self["uniform distribution"]
            
            return fn
        else:
            raise NotImplementedError(f"no function found for {analyze_result}")
