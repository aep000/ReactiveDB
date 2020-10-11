import threading
import logging
import socket
from requestHandler import handle_request 
from StorageWorker import store_data
from queue import Queue
from Database import Datastore, DerivedSettings
from transformScript import Function, Expression, ExpressionValue, FunctionOperators, ValueTypes, Union


print("Starting up server")
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as serversocket:
    db = Datastore()
    work_queue = Queue()
    serversocket.bind(("127.0.0.1", 1108))
    serversocket.listen(5)
    storage_thread = threading.Thread(target=store_data, args=(work_queue, db))
    storage_thread.start()
    # TEST TRANSFORMS
    db.add_source_table("Source1")
    db.add_source_table("Source2")
    addOne = Function([Expression(FunctionOperators.ADD,ExpressionValue(ValueTypes.FIELD, "foo"), ExpressionValue(ValueTypes.SCALAR,value=1), "foo")],"Source1", "Added")
    settingsAddOne = DerivedSettings(["Source1"], [], transform=addOne)

    union = Union(["Source2", "Added"], "Union")
    settingsUnion = DerivedSettings(["Source2", "Added"], [], transform=union)



    db.add_derived_table("Added", settingsAddOne)
    db.add_derived_table("Union", settingsUnion)
    
    #ACTUAL CODE
    while True:
        # accept connections from outside
        (clientsocket, address) = serversocket.accept()
        
        print("Connection from", address)
        clientsocket.sendall(bytes("Welcome", 'utf-8'))
        thread = threading.Thread(target=handle_request, args=(clientsocket, work_queue, db))
        thread.start()