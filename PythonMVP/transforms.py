from Database import Datastore, Transaction, TransactionMethod
from enum import Enum
from typing import List
import copy

# filter
# union & field operations
# aggregate
# scalar

class ComparisonOperators(Enum):
    EQ = 1
    LT = 2
    GT = 3
    LTE = 4
    GTE = 5
    AND = 6
    OR = 7

class FunctionOperators(Enum):
    ADD = 1
    SUBTRACT = 2
    MULT = 3
    DIV = 4
    ABS = 5

class ValueTypes(Enum):
    SCALAR = 1
    FIELD = 2
    EXPRESSION = 3

class ExpressionValue:
    def __init__(self, valueType: ValueTypes, field_name: str="", value=0, expression = None):
        self.value = value
        self.valueType = valueType
        self.field_name = field_name
        self.expression = expression
    
    def __str__(self):
        if(self.valueType == ValueTypes.SCALAR):
            return "Scalar: "+str(self.value)
        elif self.valueType == ValueTypes.EXPRESSION:
            return "Expression: "+str(self.expression)
        else:
            return "Field: "+self.field_name

    def resolve(self, datastore:Datastore, table: str, key: str):
        if(self.valueType == ValueTypes.FIELD):
            return datastore.get_table(table).get_data(key)[self.field_name]
        elif self.valueType == ValueTypes.EXPRESSION:
            return self.expression.evaluate(datastore, table, key)
        else:
            return self.value

class FunctionExpression:
    def __init__(self, operator: FunctionOperators, leftOperand:ExpressionValue, rightOperand: ExpressionValue, destField: str):
        self.leftOperand = leftOperand
        self.operator = operator
        self.rightOperand = rightOperand
        self.destField = destField

    def __str__(self):
        return "{} {} {} -> {}".format(self.leftOperand, self.operator.name, self.rightOperand, self.destField)

    def evaluate(self, datastore: Datastore, table_name: str, key: str, tempEntry):
        function = get_function(self.operator)
        tempEntry[self.destField] = function(self.leftOperand.resolve(datastore, table_name, key), self.rightOperand.resolve(datastore, table_name, key))
        return tempEntry

class ComparisonExpression:
    def __init__(self, operator: ComparisonOperators, leftOperand: ExpressionValue, rightOperand: ExpressionValue):
        self.leftOperand = leftOperand
        self.operator = operator
        self.rightOperand = rightOperand
    
    def __str__(self):
        return "{} {} {} ".format(self.leftOperand, self.operator.name, self.rightOperand)

    def evaluate(self, datastore: Datastore, table_name: str, key: str, tempEntry):
        comparison = get_compare(self.operator)
        return comparison(self.leftOperand.resolve(datastore, table_name, key), self.rightOperand.resolve(datastore, table_name, key))

class Transform:
    def run(self, datastore: Datastore, transaction: Transaction):
        pass

    def get_source_tables(self):
        pass

    def get_destination_table(self):
        pass

class Filter(Transform):
    def __init__(self, expression:ComparisonExpression, destination_table: str, source_table: str):
        self.expression = expression
        self.destination_table = destination_table
        self.source_table = source_table

    def get_source_tables(self):
        return [self.source_table]

    def get_destination_table(self):
        return self.destination_table
    
    def run(self, datastore: Datastore, transaction: Transaction):
        if(transaction.method == TransactionMethod.ADD):
            if(self.expression.evaluate(datastore, transaction.table, transaction.key, None)):
                datastore.add_data(self.destination_table, transaction.key, transaction.value)
        if(transaction.method == TransactionMethod.REMOVE):
            datastore.remove_data(self.destination_table, transaction.key)
    def __str__(self):
        return "FILTER: {} {} {}".format(self.field_name, self.comparison_operator.name, self.compare_to)

class Function(Transform):
    def __init__(self, expressions: List[FunctionExpression], destination_table: str, source_table: str):
        self.expressions = expressions
        self.destination_table = destination_table
        self.source_table = source_table

    def get_source_tables(self):
        return [self.source_table]

    def get_destination_table(self):
        return self.destination_table
    
    def run(self, datastore: Datastore, transaction: Transaction):
        if(transaction.method == TransactionMethod.ADD):
            tempEntry = copy.deepcopy(transaction.value)
            final_entry = dict()
            for expression in self.expressions:
                tempEntry = expression.evaluate(datastore, transaction.table, transaction.key, tempEntry)
                final_entry[expression.destField] = tempEntry[expression.destField]
                
            datastore.add_data(self.destination_table, transaction.key, final_entry)
        if(transaction.method == TransactionMethod.REMOVE):
            datastore.remove_data(self.destination_table, transaction.key)
    def __str__(self):
        return "FUNCTION: {} -> {}".format(str(self.expressions), self.destination_table)

class Union(Transform):
    def __init__(self, tables: List[str], destination_table: str):
        self.tables = tables
        self.destination_table = destination_table

    def get_source_tables(self):
        return self.tables

    def get_destination_table(self):
        return self.destination_table
    
    def run(self, datastore: Datastore, transaction: Transaction):
        if(transaction.method == TransactionMethod.ADD):
            existingData = copy.deepcopy(datastore.get_table(self.destination_table).get_data(transaction.key))
            if(existingData == None):
                existingData = dict()
            for key in transaction.value.keys():
                existingData[key] = transaction.value[key]
            datastore.add_data(self.destination_table, transaction.key, existingData)
        if(transaction.method == TransactionMethod.REMOVE):
            existingData = copy.deepcopy(datastore.get_table(self.destination_table).get_data(transaction.key))
            for key in datastore.get_table(transaction.table).get_data(transaction.key).keys:
                existingData.pop(key, None)
            datastore.add_data(self.destination_table, transaction.key, existingData)
    
    def __str__(self):
        return "UNION: {} -> {}".format(self.tables, self.destination_table)


def get_compare(comparison: ComparisonOperators):
    functions = {
        1: lambda x, y: x == y,
        2: lambda x, y: x < y,
        3: lambda x, y: x > y,
        4: lambda x, y: x <= y,
        5: lambda x, y: x >= y,
        6: lambda x, y: x and y,
        7: lambda x, y: x or y  
    }
    return functions[comparison.value]


def get_function(function: FunctionOperators):
    functions = {
        1: lambda x, y: x + y,
        2: lambda x, y: x - y,
        3: lambda x, y: x * y,
        4: lambda x, y: x / y,
        5: lambda x: abs(x),        
    }
    return functions[function.value]
