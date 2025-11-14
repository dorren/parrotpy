import ast
import black
import textwrap
from typing import List, Any
from ..df_spec import DfSpec
from ..inference.entity_map import EntityMap
from ..utils import fn_path

def _base_template():
    return textwrap.dedent("""
        from parrotpy import Parrot

        def generate_synthetic_data(spark):
            parrot = Parrot(spark)
            builder = parrot.df_builder()
            
            n = 100
            print(f"Starting generating {n} rows ...")
            return builder.gen_df(n)
        """)

def dict_to_ast_keywords(data_dict):
    """
    Converts a Python dictionary into a list of ast.keyword objects.

    Args:
        data_dict (dict): The dictionary to convert.

    Returns:
        list: A list of ast.keyword objects.
    """
    keywords = []
    for key, value in data_dict.items():
        # Ensure key is a string for arg
        if not isinstance(key, str):
            raise TypeError("Dictionary keys must be strings to be used as ast.keyword arguments.")

        # Convert the value to an appropriate AST node
        # This example handles basic types (str, int, float, bool, None)
        if isinstance(value, str):
            value_node = ast.Constant(value=value)
        elif isinstance(value, (int, float, bool, type(None))):
            value_node = ast.Constant(value=value)
        elif isinstance(value, list):
            value_node = ast.Constant(value=value)
        else:
            # For more complex types, you would need more elaborate AST node creation
            # For simplicity, this example raises an error for unhandled types
            raise TypeError(f"Unsupported value type for AST conversion: {type(value)}")

        keywords.append(ast.keyword(arg=key, value=value_node))
    return keywords

def build_import_stmt(fn: callable, alias: str=None):
    return ast.ImportFrom(
        module=fn.__module__,
        names=[ast.alias(name=fn.__name__, asname=None)],
        level=0
    )

class ColumnCodeGen(ast.NodeTransformer):
    """
    Generated chained function calls like below:

        builder.build_column().build_column()...
    """
    def __init__(self, target_name: str, call_name: str, df_spec: DfSpec):
        """
        Args:
            target_name: variable to be called. "builder"
            call_name: function to be called on variable, "build_column"
        """
        self.target_name = target_name
        self.call_name = call_name
        self.df_spec = df_spec
        self.entity_map = EntityMap.default()
        self.fn_imports = {}

    def _create_chained_call(self, initial_value, node):
        """
        Creates a single ast.Expr node representing the chained call.

        initial_value is existing value assigned to the variable.
        for example, RHS is the initial value.
            builder = parrot.df_builder()
        """
        # Start the chain with the initial value (e.g., the 'builder' Name node or an existing call)
        current_chain = initial_value
        fn_imports = {}
        
        for column_spec in self.df_spec.columns:
            # 1. Create the Attribute node (e.g., current_chain.build_column)
            attr = ast.Attribute(
                # The value is the result of the previous operation (the current chain)
                value=current_chain,
                attr=self.call_name,
                ctx=ast.Load()
            )
            # 2. Create the Call node (e.g., current_chain.build_column())
            # call_args = self.fn_args[i]
            # print(call_args)
            column_fn = self.entity_map[column_spec.entity_type]
            self.fn_imports[fn_path(column_fn)] = build_import_stmt(column_fn)

            fn_name = column_fn.__name__
            ast_kwargs = dict_to_ast_keywords(column_spec.kwargs)
            fn_call = ast.Expr(
                    value=ast.Call(
                        func=ast.Name(id=fn_name, ctx=ast.Load()),
                        args=ast_kwargs,
                        keywords=[]
                    )
                )
            ast_args = [
                ast.Constant(value=column_spec.name),
                ast.Constant(value=column_spec.data_type),
                fn_call
            ]
            current_chain = ast.Call(func=attr, args=ast_args, keywords=[])


        # 3. Wrap the final call in an Expression node (required for a statement)
        expr = ast.Expr(value=current_chain)
        
        # Ensure location data is consistent
        expr2 = ast.fix_missing_locations(expr)
        return expr2

    def visit_FunctionDef(self, node):
        """
        Visits a FunctionDef node, finds the injection point in its body,
        and replaces the target statement with a chained call.
        """
        new_body = []
        injected = False
        
        for statement in node.body:
            # Check for the injection point (the statement where 'builder' is defined or used)
            if not injected:
                for n in ast.walk(statement):
                    # Look for the target variable 'builder' in the statement
                    if isinstance(n, ast.Name) and n.id == self.target_name:
                        
                        if isinstance(statement, ast.Assign):
                            # Case 1: builder = DataBuilder()
                            # Replace the assignment with: builder = DataBuilder().build_column()...
                            
                            # Assume the target is the first target of the assignment
                            # This gets the 'builder' Name node from the LHS
                            target_name_node = statement.targets[0] 
                            
                            # Chain the calls onto the RHS value (e.g., DataBuilder())
                            chained_call_value = self._create_chained_call(statement.value, node).value
                            
                            # Create a new Assign node: builder = [Chained Call]
                            new_assign = ast.Assign(targets=[target_name_node], value=chained_call_value)
                            new_body.append(new_assign)
                            
                        elif isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call):
                             # Case 2: builder.some_method()
                             # This is more complex and less common for initial injection, 
                             # so we'll just inject a new statement *after* the target for simplicity.
                             new_body.append(statement) # Keep the original statement
                             
                             # Get the starting Name node for 'builder'
                             initial_name = ast.Name(id=self.target_name, ctx=ast.Load())
                             chained_expr = self._create_chained_call(initial_name, node)
                             new_body.append(chained_expr)
                             
                        else:
                            # Default Case: Inject the chained call as a new, separate statement
                            new_body.append(statement)
                            
                            # Get the starting Name node for 'builder'
                            initial_name = ast.Name(id=self.target_name, ctx=ast.Load())
                            chained_expr = self._create_chained_call(initial_name, node)
                            new_body.append(chained_expr)
                            
                        injected = True
                        break # Stop checking this statement once injection point is found
                
                # If not injected in this statement, just keep it
                if not injected:
                    new_body.append(statement)
            else:
                # If already injected, just keep the remaining statements
                new_body.append(statement)

        # Update the function's body
        node.body = new_body
        node = ast.fix_missing_locations(node)
        return node
    
    def add_imports(self, node):
        # add function imports at the top
        for import_stmt in self.fn_imports.values():
            # print(ast.unparse(import_stmt))
            node.body.insert(0, import_stmt)

        node = ast.fix_missing_locations(node)
        return node
    
def build_ast(df_spec: DfSpec):
    """ build AST node from DfSpec """
    original_tree = ast.parse(_base_template())
    code_gen = ColumnCodeGen(target_name="builder", call_name="build_column", df_spec=df_spec)
    new_tree = code_gen.visit(original_tree)
    new_tree = code_gen.add_imports(new_tree)

    return new_tree

def beautify_code(code: str):
    mode = black.Mode(target_versions={black.TargetVersion.PY311}, line_length=80)
    fmt_code = black.format_str(code, mode=mode)

    return fmt_code

def spec2code(df_spec: DfSpec) -> str:
    """ convert DfSpec to Python code """
    tree = build_ast(df_spec)
    generated_code = ast.unparse(tree)
    code = beautify_code(generated_code)

    return code
    
def spec2df(spark, df_spec: DfSpec) -> str:
    """ convert DfSpec to Python code """
    tree = build_ast(df_spec)
    code = ast.unparse(tree)
    # compile original node doesn't work because it can't handle parentheses 
    # surrounded expression, so has to parse src code.
    tree2 = ast.parse(code)

    compiled_code = compile(tree2, filename="<modified_ast>", mode="exec")
    namespace = {}
    exec(compiled_code, namespace)
    gen_fn = namespace["generate_synthetic_data"]

    return gen_fn(spark)

__all__ = ["spec2code", "spec2df"]