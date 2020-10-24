import yaml
import re
from transforms import ComparisonExpression, FunctionExpression, Filter, Function, Union, ComparisonOperators, ExpressionValue, ValueTypes, FunctionOperators
from Database import Datastore


def convert_value_from_string(type: ValueTypes, value):
    converters = {
        1: lambda x: ExpressionValue(type, value=value),
        2: lambda x: ExpressionValue(type, field_name= value)
    }
    return converters[type.value](value)

def union_parser(table_name, table_def):
    inputTables = table_def["input-tables"]
    return Union(inputTables, table_name)

def filter_parser(table_name, table_def):
    expression = parse_filter_expression_string(table_def["expression"])
    destination_table = table_name
    return Filter(expression, destination_table, table_def["source-table"])

def function_parser(table_name, table_def):
    parsed_expressions = list()
    for expression in table_def["expressions"]:
        target_expression_break = expression.split("=")
        dest_field = target_expression_break[0].strip()

        tokenize = re.compile("(\+|\-|\/|\*)")
        tokens = tokenize.split(target_expression_break[1])
        operator = tokens[1].strip()
        left = tokens[0].strip()
        right = tokens[2].strip()
        left = get_value_from_string(left)
        right = get_value_from_string(right)
        operator = get_function_operator_from_string(operator)
        exp = FunctionExpression(operator, left, right, dest_field)
        parsed_expressions.append(exp)
    return Function(parsed_expressions, table_name, table_def["source-table"])


class ExpressionNode:
    def __init__(self, operator, left, right):
        self.leftOperand = left
        self.rightOperand = right
        self.operator = operator

def parse_filter_expression_string(expression):
    paren_tokenizer = re.compile("(\(|\))")
    operator_tokenizer = re.compile("(>=|<=|<|==|>| AND | OR )")
    eval_stack = list()
    current_stack = ComparisonExpression(None, None, None)
    for token in paren_tokenizer.split(expression):
        if(token == ""):
            continue
        if(token == "("):
            eval_stack.append(current_stack)
            current_stack = ComparisonExpression(None, None, None)
        elif(token == ")"):                
            level = eval_stack.pop()
            if(level.operator != None):
                if(level.leftOperand == None):
                    level.leftOperand = ExpressionValue(ValueTypes.EXPRESSION, expression=current_stack)
                elif(level.rightOperand == None):
                    level.rightOperand = ExpressionValue(ValueTypes.EXPRESSION, expression=current_stack)
                current_stack = level
        else:
            for operator_token in operator_tokenizer.split(token):
                if(operator_token == ""):
                    continue
                if(operator_tokenizer.match(operator_token)):
                    if(current_stack.operator != None):
                        current_stack = ComparisonExpression(get_comparison_operator_from_string(operator_token), ExpressionValue(ValueTypes.EXPRESSION, expression=current_stack), None)
                    else:
                        current_stack.operator = get_comparison_operator_from_string(operator_token)
                else:
                    if(current_stack.leftOperand == None):
                        current_stack.leftOperand = get_value_from_string(operator_token.strip())
                    elif(current_stack.rightOperand == None):
                        current_stack.rightOperand = get_value_from_string(operator_token.strip())
    return current_stack

def from_parse_tree_to_expression(parse_tree: ExpressionNode):
    if(hasattr(parse_tree, "left")):
        return ComparisonExpression(parse_tree.operator, from_parse_tree_to_expression(parse_tree.leftOperand), from_parse_tree_to_expression(parse_tree.rightOperand))
    else:
        return parse_tree


        



def get_value_from_string(token):
    if(token[0]=='"'):
        return ExpressionValue(ValueTypes.SCALAR, value=token.replace('"', ''))
    elif(token[0].isalpha()):
        return ExpressionValue(ValueTypes.FIELD, field_name=token)
    elif(not token[0].isalpha()):
        return ExpressionValue(ValueTypes.SCALAR, value=float(token))
    elif(token[0] == "("):
        return 
    else:
        raise Exception("Error parsing token: ", token)

def get_function_operator_from_string(token):
    operators = {
        "+": FunctionOperators.ADD,
        "-": FunctionOperators.SUBTRACT,
        "/": FunctionOperators.DIV,
        "*": FunctionOperators.MULT
    }
    return (operators[token])

def get_comparison_operator_from_string(token):
    operators = {
        "<": ComparisonOperators.LT,
        "<=": ComparisonOperators.LTE,
        ">=": ComparisonOperators.GTE,
        ">": ComparisonOperators.GT,
        "==": ComparisonOperators.EQ,
        " AND ": ComparisonOperators.AND,
        " OR ": ComparisonOperators.OR
    }
    return operators[token]

def get_transform_parser(table_def):
    operations = {
        "union": union_parser,
        "filter": filter_parser,
        "function": function_parser
    }
    return operations[table_def["operation"]]

def load_datastore_from_file():
    with open("test.yaml") as yamlFile:
        try:
            config = yaml.safe_load(yamlFile)
            datastore = Datastore()

            for key in config.keys():
                print(config[key])
                if(config[key]["type"] == "derived"):
                    datastore.add_derived_table(key, get_transform_parser(config[key])(key, config[key]))
                else:
                    datastore.add_source_table(key)
            return datastore 
        except yaml.YAMLError as exc:
            print(exc)