import asyncio
from reactive_db_client import create_insert_request
from reactive_db_client import start_async_session
from reactive_db_client import ClientSync
from reactive_db_client import create_search_query
from json import dumps

async def test_callback(async_client):
    async def callback(event):
        print(event)
        return False

    await async_client.start_listen_non_blocking("users", "Insert", callback)
    await async_client.start_listen_non_blocking("grades", "Insert", callback)
    print(await async_client.send_request(create_search_query("FindOne", "unionTest", "matchingKey", "Bob")))
    print("After listen")
    await async_client.send_request(create_insert_request({"grade": 85, "name": "John"}, "grades"))
    await asyncio.sleep(60)


client = ClientSync("127.0.0.1", 1108)
client.connect()
client.send_request(create_insert_request({"grade": 75, "name": "Bob"}, "grades"))
client.send_request(create_insert_request({"age": 20, "name": "Bob"}, "users"))

start_async_session("127.0.0.1", 1108, test_callback)
