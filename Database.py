from enum import Enum
from typing import Dict, Any

class TableType(Enum):
    SOURCE = 1
    DERIVED = 2

class TransactionMethod(Enum):
    ADD = 1
    REMOVE = 2

class Transaction:
    def __init__(self, key: str, value, table: str, method: TransactionMethod):
        self.key = key
        self.value = value
        self.table = table
        self.method = method


class DerivedSettings:
    def __init__(self, inputs, outputs, transform = None):
        self.outputs = outputs
        self.inputs = inputs
        self.aggregate_values = dict()
        self.transform = transform
    
    def __str__(self):
        return "Inputs:{} Outputs{} Transform{}".format(self.inputs, self.outputs, self.transform)

class Table:
    def __init__(self, name: str, table_type: TableType, derived_settings: DerivedSettings=DerivedSettings([],[])):
        self.name = name
        self.table_type = table_type
        self.derived_settings = derived_settings
        self.data = dict()
    
    def __str__(self):
        return "====TABLE PRINT OUT==== Name: {name}, Type:{tableType}\nvalues:\n{values}".format(name=self.name, tableType=self.table_type, values=self.data) 

    def add_data(self, key, value):
        self.data[key] = value

    def remove_data(self, key):
        return self.data.pop(key)
    
    def get_data(self, key):
        if(key in self.data):
            return self.data[key]
        else:
            return None
    
    def add_output_table(self, table_name):
        self.derived_settings.outputs.append(table_name)

class Datastore:
    def __init__(self):
        self.tables: Dict[Table] = dict()
    def add_source_table(self, table_name):
        self.tables[table_name] = Table(table_name, TableType.SOURCE, DerivedSettings(list(), list()))
    def add_derived_table(self, table_name, transform):
        derivedSettings = DerivedSettings(transform.get_source_tables(), list(), transform)
        for inTable in transform.get_source_tables():
            self.get_table(inTable).add_output_table(table_name)
        self.tables[table_name] = Table(table_name, TableType.DERIVED, derivedSettings)
    
    def add_data(self, table_name, key, value):
        transaction = Transaction(key, value, table_name, TransactionMethod.ADD)
        table = self.get_table(table_name)
        table.add_data(key, value)
        for out_table in table.derived_settings.outputs:
            derivation_settings = self.get_table(out_table).derived_settings
            derivation_settings.transform.run(self, transaction)
    
    def remove_data(self, table_name, key):
        transaction = Transaction(key, None, table_name, TransactionMethod.REMOVE)
        for out_table in self.get_table(table_name).derived_settings.outputs:
            derivation_settings = self.get_table(out_table).derived_settings
            derivation_settings.transform.run(self, transaction)
        return self.get_table(table_name).remove_data(key)

    def get_table(self, table_name) -> Table:
        return self.tables[table_name]

#Future stuff
class PrimitiveType(Enum): 
    STRING = 1
    NUMBER = 2


class FieldType: 
    def __init__(self, name: str, primitive: PrimitiveType):
        self.name = name
        self.primitive = primitive