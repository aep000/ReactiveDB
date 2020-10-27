from fileParser import from_parse_tree_to_expression, parse_filter_expression
expression = parse_filter_expression("(java < python) OR (python > Cpp)")
expression.leftOperand