import pytest
import ast
import black
import textwrap

import parrotpy.functions as PF

@pytest.fixture
def src_code():
    return textwrap.dedent("""
        def generate_synthetic_data():
            # builder is initialized or used here
            parrot = Parrot()
            builder = parrot.df_builder()
            
            n = 100
            print(f"Starting generating {n} rows ...")
            return builder.gen_df(n)
        """)

def test_import(src_code):
    my_import = ast.ImportFrom(
        module= "parrotpy",
        names=[ast.alias(name="functions", asname="PF")],
        level=0
    )
    tree = ast.parse(src_code)
    tree.body.insert(0, my_import)
    print("--- import by alias\n" + ast.unparse(tree))



def inject_function_call(original_func_ref, module_source, import_module, import_name, call_name):
    """
    Injects a function call with a specific import into an existing function's AST.

    Args:
        original_func_ref: The original function object.
        module_source: The source code of the module containing the function.
        import_module: The module to import (e.g., 'math').
        import_name: The name to import from the module (e.g., 'sqrt').
        call_name: The name to use for the function call within the target function.
    """
    # 1. Parse the module source code into an AST
    tree = ast.parse(module_source)
    func_name = original_func_ref.__name__

    # 2. Add the import statement to the top of the module
    import_stmt = ast.ImportFrom(
        module=import_module,
        names=[ast.alias(name=import_name, asname=call_name)],
        level=0
    )
    # Insert at the beginning of the module body
    tree.body.insert(0, import_stmt)

    # 3. Define a NodeTransformer to inject the function call into the target function
    class CallInjector(ast.NodeTransformer):
        def visit_FunctionDef(self, node):
            # Check if this is the target function
            if node.name == func_name:
                # Create the new function call statement: call_name(...)
                new_call = ast.Expr(
                    value=ast.Call(
                        func=ast.Name(id=call_name, ctx=ast.Load()),
                        args=[ast.Constant(value="injected")], # Example argument
                        keywords=[]
                    )
                )
                # Insert the new call at the beginning of the function body
                node.body.insert(0, new_call)
            return self.generic_visit(node)

    transformer = CallInjector()
    new_tree = transformer.visit(tree)

    # Fix missing location attributes (lineno, col_offset)
    ast.fix_missing_locations(new_tree)
    return new_tree

def my_function():
    print("Original function body")
    return 42

def test_alias():

    # 1. Define the original module source code as a string
    original_code = textwrap.dedent("""
    def my_function():
        print("Original function body")
        return 42
    """)
    original_parsed = ast.parse(original_code)

    # 2. Define a dummy module for the imported function
    import_module_name = "injected_module"
    import sys
    import types

    injected_module = types.ModuleType(import_module_name)
    sys.modules[import_module_name] = injected_module
    def injected_call_func(arg):
        print(f"Injected function called with argument: {arg}")
    setattr(injected_module, "injected_call_func", injected_call_func)

    # 3. Inject the call
    modified_tree = inject_function_call(
        original_func_ref=my_function,
        module_source=original_code,
        import_module=import_module_name,
        import_name="injected_call_func",
        call_name="F.call_alias" # The name used inside my_function
    )
    print(ast.unparse(modified_tree))

    
    # 4. Compile and execute the modified AST to get the new function object
    compiled_code = compile(modified_tree, filename="<modified_ast>", mode="exec")
    namespace = {}
    exec(compiled_code, namespace)
    modified_function = namespace["my_function"]

    # 5. Call the modified function
    print("--- Calling modified function ---")
    result = modified_function()
    print(f"Function returned: {result}")
