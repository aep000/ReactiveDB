from queue import Queue
from Database import Datastore
def store_data(queue: Queue, database: Datastore):
    while True:
        item = queue.get()
        table = item["table"]
        key = item["key"]
        value = item["value"]
        database.add_data(table, key, value)