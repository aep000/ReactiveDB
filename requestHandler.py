import socket
import json
from queue import Queue
from Database import Datastore

def handle_request(client_socket: socket.socket, queue: Queue, database: Datastore):
    request = ""
    while True:
        buff = client_socket.recv(1)
        if(buff == ''):
            break
        request += buff.decode('utf-8')
        if(request[-1] == '~'):
            requestJSON = json.loads(request[:-1])
            if(requestJSON["method"]=="put"):
                queue.put(requestJSON)
                client_socket.sendall(bytes("Success!\n", "utf-8"))
            elif(requestJSON["method"]=="get"):
                client_socket.sendall(bytes("\n"+json.dumps(database.get_table(requestJSON["table"]).get_data(requestJSON["key"]))+"\n", "utf-8"))
            elif(requestJSON["method"] == "getAll"):
                client_socket.sendall(bytes("\n"+database.get_table(requestJSON["table"]).__str__()+"\n", "utf-8"))
            else:
                client_socket.sendall(bytes("Error with request\n", "utf-8"))
            request = ""
            requestJSON = dict()
    client_socket.close()
