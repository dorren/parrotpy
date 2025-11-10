from collections import UserDict

class FunctionMap(UserDict):
    def register(self, name: str, fn:callable):
        super().__setitem__(name, fn)

    def match(self, analyze_result: dict):
        """ find the right function for the DF analyze result """
        if "distribution" in analyze_result:
            dist_type = analyze_result["distribution"]

            if dist_type == "norm":
                fn = self["distribution.norm"]
            elif dist_type == "uniform":
                fn = self["distribution.uniform"]
            
            return fn
        else:
            raise NotImplementedError(f"no function found for {analyze_result}")
