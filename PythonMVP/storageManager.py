import os
import heapq

BLOCK_DATA_SIZE = 500
REFERENCE_SIZE = 32
BLOCK_SIZE = BLOCK_DATA_SIZE + REFERENCE_SIZE
EMPTY_REFERENCE = bytes(REFERENCE_SIZE)
EMPTY_BLOCK = bytes(BLOCK_SIZE)

class StorageManager:
    def __init__(self, file_name) -> None:
        self.file_name = file_name
        if not os.path.exists(file_name):
            with open(file_name, 'w'): pass

        self.number_of_blocks = os.path.getsize(file_name)/BLOCK_SIZE
        self.open_blocks = []

    
    def _read_block(self, block_number, file_pointer):
        file_pointer.seek(BLOCK_SIZE*block_number, 0)
        block = file_pointer.read(BLOCK_SIZE)
        return block

    def _write_block(self, block_number, to_write: bytes, file_pointer):
        file_pointer.seek(BLOCK_SIZE*block_number, 0)
        if(len(to_write)<BLOCK_SIZE):
            to_write = bytes(BLOCK_SIZE - len(to_write)) + to_write
        file_pointer.write(to_write)
        return True
    def _delete_block(self, block_number, file_pointer):
        heapq.heappush(self.open_blocks, block_number)
        self._write_block(block_number, EMPTY_BLOCK, file_pointer)

    def allocate_block(self):
        if(len(self.open_blocks)>0):
            return int(heapq.heappop(self.open_blocks))
        else:
            self.number_of_blocks+=1
            return int(self.number_of_blocks)

    def write_data(self, data: bytes, block=-1):
        with open(self.file_name, "r+b") as f:
            if(block == -1):
                block = self.allocate_block()
            else:
                if(block in self.open_blocks):
                    self.open_blocks.remove(block)

            root_block = block
            cursor = 0
            while(cursor<len(data)):
                data_to_write = data[cursor:cursor+BLOCK_DATA_SIZE]
                cursor = cursor+BLOCK_DATA_SIZE
                if(cursor>len(data)):
                    self._write_block(block, data_to_write+EMPTY_REFERENCE, f)
                else:
                    next_block = self.allocate_block()
                    self._write_block(block, data_to_write+next_block.to_bytes(REFERENCE_SIZE, "big"), f)
                    block = next_block
            return root_block
    
    def read_data(self, root_block):
        with open(self.file_name, "rb") as f:
            output = b""
            while True:
                raw_block = self._read_block(root_block, f)
                next_block = int.from_bytes(raw_block[-REFERENCE_SIZE:],"big")
                output += raw_block[:BLOCK_DATA_SIZE].lstrip(b'\x00')
                if(next_block == 0):
                    break
                else:
                    root_block = next_block
            return output
    
    def delete_data(self, root_block):
        with open(self.file_name, "r+b") as f:
            while True:
                raw_block = self._read_block(root_block, f)
                next_block = int.from_bytes(raw_block[-REFERENCE_SIZE:],"big")
                self._delete_block(root_block, f)
                if(next_block == 0):
                    break
                else:
                    root_block = next_block
