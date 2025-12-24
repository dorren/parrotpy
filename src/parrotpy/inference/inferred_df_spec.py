import ast
from ..models import DfSpec
from ..code_gen.column_code_gen import build_ast, beautify_code

class InferredDfSpec(DfSpec):
    def to_code(self) -> str:
        """ convert DfSpec to Python code """
        tree = build_ast(self)
        generated_code = ast.unparse(tree)
        code = beautify_code(generated_code)

        return code
        
    def to_df(self, spark) -> str:
        """ convert DfSpec to Python code """
        tree = build_ast(self)
        code = ast.unparse(tree)
        # compile original node doesn't work because it can't handle parentheses 
        # surrounded expression, so has to parse src code.
        tree2 = ast.parse(code)

        compiled_code = compile(tree2, filename="<modified_ast>", mode="exec")
        namespace = {}
        exec(compiled_code, namespace)
        gen_fn = namespace["generate_synthetic_data"]

        return gen_fn(spark)