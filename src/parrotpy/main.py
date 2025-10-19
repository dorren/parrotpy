

class Parrot:
    def __init__(self):
        lib_name = __package__  # parrotpy

        modules = ["common", "stats"]
        for mod in modules:
            mod_name = f"{lib_name}.{mod}"
            setattr(self, mod, __import__(mod_name, fromlist=[""]))

