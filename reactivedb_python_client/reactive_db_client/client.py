from asyncio.streams import StreamReader, StreamWriter
import json
import socket
import asyncio

class ClientSync:
    host: str
    port: int
    connection:socket = None
    def __init__(self, host, port):
        self.host = host
        self.port = port
    
    def connect(self):
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connection.connect((self.host, self.port))

    def send_request(self, request):
        if self.connection != None:
            request_string = json.dumps(request)
            message_size = len(request_string).to_bytes(4, byteorder='big')
            self.connection.send(message_size)
            self.connection.send(bytes(request_string,"utf-8"))
            response_size = int.from_bytes(self.connection.recv(4), byteorder='big')
            response = self.connection.recv(response_size).decode("utf-8")
            return json.loads(response)
        else:
            raise Exception("Connection not open")
    
    def start_listen_blocking(self, table_name, event, callback):
        if self.connection != None:
            assert event == "Insert" or event == "Delete"
            request = {"StartListen":{"table_name":table_name, "event": event}}
            request_string = json.dumps(request)
            message_size = len(request_string).to_bytes(4, byteorder='big')
            self.connection.send(message_size)
            self.connection.send(bytes(request_string,"utf-8"))
            while True:
                response_size = int.from_bytes(self.connection.recv(4), byteorder='big')
                response = self.connection.recv(response_size).decode("utf-8")
                if callback(json.loads(response)):
                    break
        else:
            raise Exception("Connection not open")

def start_async_session(host, port, callback):
    client = ClientAsync(host, port)
    asyncio.run(client.connect(callback))

class ClientAsync:
    host: str
    port: int
    reader:StreamReader = None
    write:StreamWriter = None
    event_queues = dict()
    response_queues = dict()
    connection = True
    def __init__(self, host, port):
        self.host = host
        self.port = port
    
    async def connect(self, callback):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        asyncio.create_task(self.handle_responses())
        await asyncio.create_task(callback(self))

    
    async def handle_responses(self):
        while True:
            response_size = int.from_bytes(await self.reader.read(4), byteorder='big')
            response = json.loads((await self.reader.read(response_size)).decode("utf-8"))
            if "Event" in response:
                self.event_queues[(response["Event"]["table_name"], response["Event"]["event"])].put_nowait(response)
            else:
                self.response_queues[response["RequestResponse"]["request_id"]].put_nowait(response)

    async def send_request(self, request):
        if self.connection != None:
            request_string = json.dumps(request)
            message_size = len(request_string).to_bytes(4, byteorder='big')
            request_id = request["Query"]["request_id"]
            self.response_queues[request_id] = asyncio.Queue()
            self.writer.write(message_size)
            self.writer.write(bytes(request_string,"utf-8"))
            response = await self.response_queues[request_id].get()
            self.response_queues[request_id].task_done()
            return response
        else:
            raise Exception("Connection not open")
    
    async def start_listen_blocking(self, table_name, event, callback):
        if self.connection != None:
            assert event == "Insert" or event == "Delete"
            request = {"StartListen":{"table_name":table_name, "event": event}}
            self.event_queues[(table_name, event)] = asyncio.Queue()
            request_string = json.dumps(request)
            message_size = len(request_string).to_bytes(4, byteorder='big')
            self.writer.write(message_size)
            self.writer.write(bytes(request_string,"utf-8"))
            while True:
                print("Doing something")
                await asyncio.sleep(1)
                response = await self.event_queues[(table_name, event)].get()
                self.event_queues[(table_name, event)].task_done()
                if await callback(response):
                    break
        else:
            raise Exception("Connection not open")
    
    async def start_listen_non_blocking(self, table_name, event, callback):
        asyncio.create_task(self.start_listen_blocking(table_name, event, callback))


    
