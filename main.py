from fileParser import load_datastore_from_file
import threading
import logging
import socket
from requestHandler import handle_request 
from StorageWorker import store_data
from queue import Queue
from Database import Datastore


print("Starting up server")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serversocket:
    db = load_datastore_from_file()
    work_queue = Queue()
    serversocket.bind(("127.0.0.1", 1109))
    serversocket.listen(5)
    storage_thread = threading.Thread(target=store_data, args=(work_queue, db))
    storage_thread.start()
    
    while True:
        # accept connections from outside
        (clientsocket, address) = serversocket.accept()
        
        print("Connection from", address)
        clientsocket.sendall(bytes("Welcome", 'utf-8'))
        thread = threading.Thread(target=handle_request, args=(clientsocket, work_queue, db))
        thread.start()