from storageManager import StorageManager
import bson
import random
import bisect

def node_value_binary_search(arr, x): 
    pos = 0
    while pos < len(arr) and x.index > arr[pos].index:
        pos +=1
    return pos

    '''low = 0
    high = len(arr) - 1
    mid = 0
  
    while low <= high: 
        mid = int((high + low) / 2)
        if arr[mid].index < x.index: 
            low = mid + 1
            mid += 1
        elif arr[mid].index > x.index: 
            high = mid - 1
            mid = mid - 1
        else: 
            return mid 
    return mid'''

class _Node:
    def __init__(self, size, is_leaf):
        self.size = size
        self.values = list()
        self.next = -1
        self.last = None
        self.leaf = is_leaf

    def entry_is_in_node(self, entry):
        if entry.index >= self.values[0] and entry.index <= self.values[-1]:
            return 0
        elif entry.index >= self.values[0]:
            return 1
        else:
            return -1
    
    def get_entries(self, index):
        outputs = []
        pos = 0
        while pos< len(self.values) and self.values[pos] <= index:
            if(self.values[pos] == index):
                outputs.append(self.values[pos])
            pos+=1

        return outputs
    
    def is_full(self):
        return len(self.values) >= self.size - 1
    
    def is_leaf(self):
        return self.leaf
    
    def insert(self, entry):
        pos = 0
        while pos < len(self.values) and entry.index > self.values[pos].index:
            pos +=1
        self.values.insert(pos, entry)
        return pos
    
    def split(self):
        median = int(len(self.values)/2)
        left = self.values[:median]
        right = self.values[median:]
        median_val = self.values[median]
        return (left, right, median, median_val)

    def serialize(self):
        node_type = "reference"
        if(self.leaf):
            node_type = "leaf"
        values = list(map(lambda x: x.get_dict(), self.values))
        return bson.dumps({"type": node_type, "entries": values, "next": self.next, "last": self.last, "size": self.size})
    
    @staticmethod
    def deserialize(data:bytes):
        obj = bson.loads(data)
        node = _Node(obj["size"], obj["type"] == "leaf")
        node.values = [NodeValue.from_dict(x) for x in obj["entries"]]
        node.next = obj["next"]
        node.last = obj["last"]
        return node

    def __str__(self) -> str:
        leaf = ""
        if(self.leaf):
            leaf = "Leaf "
        output = leaf+"Node["
        for value in self.values:
            output += str(value)+", "
        output += "]"
        return output

class NodeValue:
    def __eq__(self, other) -> bool:
        return self.index == other.index
    def __lt__(self, other) -> bool:
        return self.index < other.index
    def __le__(self, other) -> bool:
        return self.index <= other.index
    def __gt__(self, other) -> bool:
        return self.index > other.index
    def __ge__(self, other) -> bool:
        return self.index >= other.index
    def __ne__(self, other) -> bool:
        if(other == None):
            return True
        return self.index != other.index
    @staticmethod
    def from_dict(raw):
        if("left" in raw):
            return Reference(raw["index"], raw["left"], raw["right"])
        else:
            return Entry(raw["index"], raw["value"])


def get_node_from_storage(block: int, storage_manager: StorageManager) -> _Node:
    encoded_node = storage_manager.read_data(block)
    return _Node.deserialize(encoded_node)

class Reference(NodeValue):
    def __init__(self, index, left, right):
        self.index = index
        self.left = left
        self.right = right
    def __str__(self) -> str:
        return "Index: {}, left: {}, right: {}".format(self.index, get_node_from_storage(self.left, StorageManager("testIndex.txt")), get_node_from_storage(self.right, StorageManager("testIndex.txt")))
    
    def keys(self):
        return ["index", "left", "right"]
    def __getitem__(self, key):
        decoder = {
            "index": self.index,
            "left": self.left,
            "right": self.right
        }
        return decoder[key]
    def get_dict(self):
        return {
            "index": self.index,
            "left": self.left,
            "right": self.right
        }

class Entry(NodeValue):
    #__lt__, __le__, __gt__, __ge__, __eq__ and __ne__
    def __init__(self, index, value):
        self.index = index
        self.value = value
    def __str__(self) -> str:
        return "Entry: {}".format(self.index)
    
    def keys(self):
        return ["index", "value"]
    def __getitem__(self, key):
        decoder = {
            "index": self.index,
            "value": self.value
        }
        return decoder[key]
    def get_dict(self):
        return {
            "index": self.index,
            "value": self.value
        }
def get_node_from_storage(block: int, storage_manager: StorageManager) -> _Node:
    encoded_node = storage_manager.read_data(block)
    return _Node.deserialize(encoded_node)

def update_node_field(block, field, value, storage_manager: StorageManager):
    node = get_node_from_storage(block, storage_manager)
    setattr(node, field, value)
    storage_manager.delete_data(block)
    storage_manager.write_data(node.serialize(), block= block)

def update_node(block, node: _Node, storage_manager: StorageManager):
    storage_manager.delete_data(block)
    storage_manager.write_data(node.serialize(), block= block)



def insert_new_node(node: _Node, storage_manager: StorageManager):
    return storage_manager.write_data(node.serialize())


class BPlusTree:
    def __init__(self, node_size, file_name):
        self.node_size = node_size
        self.file_name = file_name
        self.storage_manager = StorageManager(file_name)
    
    def insert(self, entry):
        def insert_entry(node_ref: int, entry: Entry):
            node = get_node_from_storage(node_ref, self.storage_manager)
            if(node.is_leaf()):
                if(node.is_full()):
                    left, right, median, median_val = node.split()
                    next_node_ptr = self.storage_manager.get_block()
                    node.values = left
                    next_node = _Node(node.size, True)
                    next_node.values = right
                    if(median_val.index <= entry.index):
                        next_node.insert(entry)
                    else:
                        node.insert(entry)
                    
                    if(node.next != -1):
                        next_node.next = node.next
                        update_node_field(node.next, "last", next_node_ptr, self.storage_manager)
                    
                    if(node_ref == 1):
                        node_ref = self.storage_manager.get_block()

                    node.next = next_node_ptr
                    next_node.last = node_ref
                    update_node(node_ref, node, self.storage_manager)
                    update_node(next_node_ptr, next_node, self.storage_manager)
                    return Reference(median_val.index, node_ref, next_node_ptr)
                else:
                    node.insert(entry)
                    update_node(node_ref, node, self.storage_manager)
                    return None
            else:
                next_index = node_value_binary_search(node.values, entry)
                if(next_index >= len(node.values)):
                    next_index -= 1
                if(next_index < 0):
                    next_index = 0
                next_node_down_ref = node.values[next_index].left
                if(entry >= node.values[next_index]):
                    next_node_down_ref = node.values[next_index].right
                # Get next node from storage
                
                ref_to_insert = insert_entry(next_node_down_ref, entry)
                if(ref_to_insert != None):
                    if(node.is_full()):
                        left, right, median, median_val = node.split()
                        node.values = left
                        next_node = _Node(self.node_size, False)
                        next_node.values = right

                        next_node_ptr = insert_new_node(next_node, self.storage_manager)
                        if(node_ref == 1):
                            node_ref = self.storage_manager.get_block()
                        update_node(node_ref, node, self.storage_manager)
                        return Reference(median_val.index, node_ref, next_node_ptr)
                    else:
                        position = node.insert(ref_to_insert)
                        if(position+1 < len(node.values)):
                            node.values[position+1].left = ref_to_insert.right
                        update_node(node_ref, node, self.storage_manager)
                        return None
                else:
                    return None
        root = get_node_from_storage(1, self.storage_manager)
        ref_to_insert = insert_entry(1, entry)
        if(ref_to_insert != None):
            if(ref_to_insert.left == 1):
                old_root = get_node_from_storage(1, self.storage_manager)
                new_block = insert_new_node(old_root, self.storage_manager)
                self.storage_manager.delete_data(1)
                ref_to_insert.left = new_block
            root = _Node(self.node_size, False)
            root.insert(ref_to_insert)
            update_node(1, root, self.storage_manager)
        
    def exact_search(self, index,):
        def search_helper(node_ref: int, entry):
            node = get_node_from_storage(node_ref, self.storage_manager)
            if(node.is_leaf()):
                return node.get_entries(entry)
            else:
                next_index = node_value_binary_search(node.values, entry)
                next_node_down = node.values[next_index].left
                if(entry >= node.values[next_index]):
                    next_node_down = node.values[next_index].right
                return search_helper(next_node_down, entry)
            
        return search_helper(1, Entry(index, "nothing"))

    def __str__(self) -> str:
        return "{}".format(get_node_from_storage(1, self.storage_manager))



entries = []
indexes = [*range(20)]
random.shuffle(indexes)
for n in indexes:
    entries.append(Entry(n, "test "+str(n)))

node = _Node(20, True)
node.insert(Entry(1, "test"))
storage_manager = StorageManager("another_test.txt")
l = storage_manager.write_data(node.serialize())
#print(get_node_from_storage(l, storage_manager))

storage_manager = StorageManager("testIndex.txt")
bPTree = BPlusTree(5, "testIndex.txt")
'''for entry in entries:
    print(bPTree)
    print ("=========================================")
    bPTree.insert(entry)'''

print(bPTree)
print(bPTree.exact_search(5)[0].value)