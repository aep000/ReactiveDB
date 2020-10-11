from Database import Datastore, Transaction, TransactionMethod, Table
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

class FunctionOperators(Enum):
    ADD = 1
    SUBTRACT = 2
    MULT = 3
    DIVL = 4
    DIVR = 5
    ABS = 6

class ValueTypes(Enum):
    SCALAR = 1
    FIELD = 2

class ExpressionValue:
    def __init__(self, valueType: ValueTypes, field_name: str="", value=0):
        self.value = value
        self.valueType = valueType
        self.field_name = field_name
    
    def __str__(self):
        if(self.valueType == ValueTypes.SCALAR):
            return "Scalar: "+str(self.value)
        else:
            return "Field: "+self.field_name

    def resolve(self, datastore:Datastore, table: str, key: str):
        if(self.valueType == ValueTypes.FIELD):
            return datastore.get_table(table).get_data(key)[self.field_name]
        else:
            return self.value

class Expression:
    def __init__(self, operator: FunctionOperators, leftOperand:ExpressionValue, rightOperand: ExpressionValue, destField: str):
        self.leftOperand = leftOperand
        self.operator = operator
        self.rightOperand = rightOperand
        self.destField = destField

    def __str__(self):
        return "{leftOperand} {operator} {rightOperand}".format(self.leftOperand, self.operator.name, self.rightOperand)

    def evaluate(self, datastore: Datastore, table_name: str, key: str, tempEntry):
        function = get_function(self.operator)
        tempEntry[self.destField] = function(self.leftOperand.resolve(datastore, table_name, key), self.rightOperand.resolve(datastore, table_name, key))
        return tempEntry

class Transform:
    def run(self, datastore: Datastore, transaction: Transaction):
        pass

class Filter(Transform):
    def __init__(self, field_name: str, comparison_operator: ComparisonOperators, compare_to: ExpressionValue, source_table: str, destination_table: str):
        self.field_name = field_name
        self.comparison_operator = comparison_operator
        self.compare_to = compare_to
        self.source_table = source_table
        self.destination_table = destination_table
    
    def  run(self, datastore: Datastore, transaction: Transaction):
        if(transaction.method == TransactionMethod.ADD):
            comparison = get_compare(self.comparison_operator)
            if(comparison(transaction.value[self.field_name], self.compare_to.resolve(datastore, transaction.table, transaction.table))):
                datastore.add_data(self.destination_table, transaction.key, transaction.value)
        if(transaction.method == TransactionMethod.REMOVE):
            datastore.remove_data(self.destination_table, transaction.key)

class Function(Transform):
    def __init__(self, expressions: List[Expression], source_table: str, destination_table: str):
        self.expressions = expressions
        self.source_table = source_table
        self.destination_table = destination_table
    
    def run(self, datastore: Datastore, transaction: Transaction):
        if(transaction.method == TransactionMethod.ADD):
            tempEntry = copy.deepcopy(transaction.value)
            for expression in self.expressions:
                tempEntry = expression.evaluate(datastore, transaction.table, transaction.key, tempEntry)
            datastore.add_data(self.destination_table, transaction.key, tempEntry)
        if(transaction.method == TransactionMethod.REMOVE):
            datastore.remove_data(self.destination_table, transaction.key)

class Union(Transform):
    def __init__(self, tables: List[str], destination_table: str):
        self.tables = tables
        self.destination_table = destination_table
    
    def run(self, datastore: Datastore, transaction: Transaction):
        existingData = copy.deepcopy(datastore.get_table(self.destination_table).get_data(transaction.key))
        if(existingData == None):
            existingData = dict()
        for key in transaction.value.keys():
            existingData[key] = transaction.value[key]
        datastore.add_data(self.destination_table, transaction.key, existingData)

def get_compare(comparison: ComparisonOperators):
    functions = {
        1: lambda x, y: x == y,
        2: lambda x, y: x < y,
        3: lambda x, y: x > y,
        4: lambda x, y: x <= y,
        5: lambda x, y: x >= y,        
    }
    return functions[comparison.value]


def get_function(function: FunctionOperators):
    functions = {
        1: lambda x, y: x + y,
        2: lambda x, y: x - y,
        3: lambda x, y: x * y,
        4: lambda x, y: x / y,
        5: lambda x, y: y / x,
        6: lambda x: abs(x),        
    }
    return functions[function.value]
