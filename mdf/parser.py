"""
Parser for parsing the magic function command line arguments and
also used for parsing source code to look for varnode declarations.

Parses strings such as::

    2012-01-01 datetime.now() node_defintion list[index] [shifts,another_shift]

Arguments may be comma or space separated.

This isn't adhere strictly to the Python grammar, it's just matched
opening and closing braces to get lists of what look like Python
expressions.
"""
from pyparsing import *
import inspect

expr = Forward()

name = Word(alphanums + "_")
integer = Word(nums)
number = Word(nums) + Optional(".") + Optional(integer)
date = Regex("\d{4}-\d{2}-\d{2}")
num_processors = Regex("\|\|\s*\d+")

named_parameter = name + "=" + (number | QuotedString("'") | QuotedString('"') | expr)
dict_parameter = name + ":" + (number | QuotedString("'") | QuotedString('"') | expr)

enclosed = Forward()
nested_parens = nestedExpr("(", ")", content=enclosed).leaveWhitespace()
nested_brackets = nestedExpr("[", "]", content=enclosed).leaveWhitespace()
nested_curlies = nestedExpr("{", "}", content=enclosed).leaveWhitespace()
enclosed << OneOrMore(named_parameter | dict_parameter |  expr | ",")

decl = (nested_parens | nested_brackets | nested_curlies | date | name | num_processors)
func = (decl + nested_parens).leaveWhitespace()
item = (decl + nested_brackets).leaveWhitespace()

#
# get_node_assignment_expr("varnode") matches lines such as
#
# my_var = varnode(default=1.0)
#
def get_node_assignment_expr(nodetype="varnode"):
    node_expr = nodetype + Optional(White()) + nested_parens
    node_decl_expr = name + "=" + node_expr + Optional(pythonStyleComment)
    return node_decl_expr

# e.g. decl.func()[item].func2()
sub_expr = Forward()
sub_expr << (Or((func, item, decl)) + ZeroOrMore(Optional(".") + sub_expr)).leaveWhitespace()

# e.g. decl.func()[item].func2() + 1
expr << sub_expr + ZeroOrMore(oneOf("+ - * & | ^ %") + expr)

def tokenize(input_str, parser_element=expr):
    """
    returns a list of python expressions from the input string.
    e.g. using the default parser_element, expr::

        tokenize("2010-01-01 datetime.now() node_a node_b.delaynode(periods=1) [a, b, c]")

    returns::

        ["2010-01-01", "datetime.now()", "node_a", "node_b.delaynode(periods=1"), "[a, b, c]"]

    The returned tokens may not be valid Python expressions but if the input is
    sensible they will be.
    """
    return [input_str[start:end].strip() for (toks, start, end) in parser_element.scanString(input_str)]

def get_assigned_node_name(nodetype="varnode", stack_level=1):
    """
    looks for a node's name by looking down the stack and parsing the source code
    to find expressions like::

        x = varnode(args)

    Returns the variable name on the left hand side of the assignment, or raises
    an AssertionError..

    stack_level is the number of levels down the stack to look for the assignment.
    If calling from a cython'd function it shouldn't include that level (not including
    the frame of this function call).
    """
    decl_expr = get_node_assignment_expr(nodetype)
    stack = inspect.stack(context=30)

    record = stack[stack_level+1]
    context, last_line = record[4], record[5]

    if context is not None:
        # work backwards through the context to find the start of the assignment
        tokens = []
        current_line = last_line
        statement = ""
        while current_line >= 0 and not tokens:
            statement = context[current_line] + statement
            tokens = tokenize(statement, decl_expr)
            current_line -= 1

        if len(tokens) == 1:
            # check that what matched was actually the line we were on, and not another varnode
            # declaration from earlier in the context
            if tokens[0].strip() != statement.strip():
                if tokens[0].strip() != statement.strip():
                    raise AssertionError(("Error parsing line looking for %s name:\n" % nodetype) +
                                         "'%s' != '%s'" % (tokens[0].strip(), statement.strip()))

            # split the line into the tokens making up the node declaration, and the first
            # one is always the variable name
            sub_tokens, start, end = next(decl_expr.scanString(statement))
            name = sub_tokens[0]
            return name

        raise AssertionError(("Could not infer name for %s. " % nodetype) +
                             ("Line '%s' doesn't look like a %s assignment" %
                                 (context[last_line].strip(), nodetype)))

    raise AssertionError(("Could not infer name for %s. " % nodetype) +
                         ("No source code found - is the code cythoned?"))
