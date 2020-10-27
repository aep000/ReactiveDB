from storageManager import StorageManager


class CacheingStorageManager(StorageManager):
    def __init__(self, file_name, cache_size):
        self.cache = dict()
        self.cache_size = cache_size
        super().__init__(file_name)

    def write_data(self, data: bytes, block=-1):
        if(block != -1 and block in self.cache):
            self.cache[block] = data