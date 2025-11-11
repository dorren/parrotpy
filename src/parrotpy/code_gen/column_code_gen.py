import ast
import black
import textwrap
from typing import List, Any
from ..df_spec import DfSpec

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
        self.num_injections = len(df_spec.columns)

    def _create_chained_call(self, initial_value):
        """
        Creates a single ast.Expr node representing the chained call.

        initial_value is existing value assigned to the variable.
        for example, RHS is the initial value.
            builder = parrot.df_builder()
        """
        # Start the chain with the initial value (e.g., the 'builder' Name node or an existing call)
        current_chain = initial_value
        
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
            ast_kwargs = dict_to_ast_keywords(column_spec.kwargs)
            ast_args = [
                ast.Constant(value=column_spec.name),
                ast.Constant(value=column_spec.data_type),
                *ast_kwargs
            ]
            current_chain = ast.Call(func=attr, args=ast_args, keywords=[])
        
        # 3. Wrap the final call in an Expression node (required for a statement)
        expr = ast.Expr(value=current_chain)
        
        # Ensure location data is consistent
        ast.fix_missing_locations(expr)
        return expr

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
                            chained_call_value = self._create_chained_call(statement.value).value
                            
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
                             chained_expr = self._create_chained_call(initial_name)
                             new_body.append(chained_expr)
                             
                        else:
                            # Default Case: Inject the chained call as a new, separate statement
                            new_body.append(statement)
                            
                            # Get the starting Name node for 'builder'
                            initial_name = ast.Name(id=self.target_name, ctx=ast.Load())
                            chained_expr = self._create_chained_call(initial_name)
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
        ast.fix_missing_locations(node)
        return node