
def node_value_binary_search(arr, x): 
    low = 0
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
    return mid

class _Node:
    def __init__(self, size, is_leaf):
        self.size = size
        self.values = list()
        self.next = None
        self.last = None
        self.leaf = is_leaf

    def entry_is_in_node(self, entry):
        if entry.index >= self.values[0] and entry.index <= self.values[-1]:
            return 0
        elif entry.index >= self.values[0]:
            return 1
        else:
            return -1
    
    def get_entry(self, index):
        return self.values[node_value_binary_search(self.values, index)]
    
    def is_full(self):
        return len(self.values) >= self.size - 1
    
    def is_leaf(self):
        return self.leaf
    
    def insert(self, entry):
        position = node_value_binary_search(self.values, entry)
        self.values.insert(position, entry)
        return position
    
    def split(self):
        median = int(len(self.values)/2)
        left = self.values[:median]
        right = self.values[median:]
        median_val = self.values[median]
        return (left, right, median, median_val)
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

class Reference(NodeValue):
    def __init__(self, index, left, right):
        self.index = index
        self.left = left
        self.right = right
    def __str__(self) -> str:
        return "Index: {}, left: {}, right: {}".format(self.index, self.left, self.right)

class Entry(NodeValue):
    #__lt__, __le__, __gt__, __ge__, __eq__ and __ne__
    def __init__(self, index, value):
        self.index = index
        self.value = value
    def __str__(self) -> str:
        return "Entry: {}".format(self.index)

class BPlusTree:
    def __init__(self, node_size):
        self.node_size = node_size
        self.root = _Node(node_size, True)
    
    def insert(self, entry):
        def insert_entry(node:_Node, entry: Entry):
            if(node.is_leaf()):
                if(node.is_full()):
                    left, right, median, median_val = node.split()
                    node.values = left
                    next_node = _Node(node.size, True)
                    next_node.values = right
                    if(median_val.index <= entry.index):
                        next_node.insert(entry)
                    else:
                        node.insert(entry)
                    if(node.next != None):
                        next_node.next = node.next
                        node.next.last = next_node
                    node.next = next_node
                    next_node.last = node
                    return Reference(median_val.index, node, next_node)
                else:
                    node.insert(entry)
                    return None
            else:
                next_index = node_value_binary_search(node.values, entry)
                if(next_index >= len(node.values)):
                    next_index -= 1
                if(next_index < 0):
                    next_index = 0
                next_node_down = node.values[next_index].left
                if(entry >= node.values[next_index]):
                    next_node_down = node.values[next_index].right
                
                ref_to_insert = insert_entry(next_node_down, entry)
                if(ref_to_insert != None):
                    if(node.is_full()):
                        left, right, median, median_val = node.split()
                        node.values = left
                        next_node = _Node(self.node_size, False)
                        next_node.values = right
                        return Reference(median_val.index, node, next_node)
                    else:
                        position = node.insert(ref_to_insert)
                        if(position+1 < len(node.values)):
                            node.values[position+2].left = ref_to_insert.right
                        return None
                else:
                    return None
        ref_to_insert = insert_entry(self.root, entry)
        if(ref_to_insert != None):
            self.root = _Node(self.node_size, False)
            self.root.insert(ref_to_insert)
        
    def search(self, index,):
        def search_helper(node: _Node, entry):
            if(node.is_leaf()):
                return node.get_entry(entry)
            else:
                next_index = node_value_binary_search(node.values, entry)
                if(next_index >= len(node.values)):
                    next_index -= 1
                if(next_index < 0):
                    next_index = 0
                next_node_down = node.values[next_index].left
                if(entry >= node.values[next_index]):
                    next_node_down = node.values[next_index].right
                return search_helper(next_node_down, entry)
            
        return search_helper(self.root, Entry(index, "nothing"))
    
    def __str__(self) -> str:
        return "{}".format(self.root)


entries = []
for n in range(10):
    entries.append(Entry(n, "test"))

node = _Node(10, True)

bPTree = BPlusTree(4)
for entry in entries:
    bPTree.insert(entry)
print(bPTree)