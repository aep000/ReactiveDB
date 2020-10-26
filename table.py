from storageManager import StorageManager
from bptree import BPlusTree, Entry
import bson
from enum import Enum

class DerivedSettings:
    def __init__(self, inputs, outputs, transform = None):
        self.outputs = outputs
        self.inputs = inputs
        self.aggregate_values = dict()
        self.transform = transform
    
    def __str__(self):
        return "Inputs:{} Outputs{} Transform{}".format(self.inputs, self.outputs, self.transform)

class TableType(Enum):
    SOURCE = 1
    DERIVED = 2

class PersistentTable:
    def __init__(self, name: str, table_type: TableType, derived_settings: DerivedSettings=DerivedSettings([],[])):
        self.name = name
        self.table_type = table_type
        self.derived_settings = derived_settings
        self.index_file = "db/{}.index".format(name)
        self.data_file = "db/{}.table".format(name)
        self.index = BPlusTree(5, self.index_file)
        self.storage_manager = StorageManager(self.data_file)
    
    def __str__(self):
        return "====TABLE PRINT OUT==== Name: {name}, Type:{tableType}\nvalues:\n{values}".format(name=self.name, tableType=self.table_type, values=[bson.loads(self.storage_manager.read_data(e.value)) for e in self.index.get_all()]) 

    def add_data(self, key, value):
        block = self.storage_manager.write_data(bson.dumps(value))
        self.index.insert(Entry(key, block))

    def remove_data(self, key):
        raise Exception("NOT IMPLIMENTED EXCEPTION")
        #return self.data.pop(key)
    
    def get_data(self, key):
        entry = self.index.exact_search(key)
        if(len(entry) == 0):
            return None
        else:
            return bson.loads(self.storage_manager.read_data(entry[0].value))
        
    def add_output_table(self, table_name):
        self.derived_settings.outputs.append(table_name)