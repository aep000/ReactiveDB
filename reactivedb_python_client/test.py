import asyncio
from reactive_db_client.client import start_async_session
from reactive_db_client import client
from reactive_db_client import create_search_query, ClientAsync
from json import dumps

async def test_callback(async_client):
    async def callback(event):
        print(event)
        return False

    await async_client.start_listen_non_blocking("users", "Insert", callback)
    await async_client.start_listen_non_blocking("grades", "Insert", callback)
    print(await async_client.send_request(create_search_query("FindOne", "unionTest", "matchingKey", "Alex")))
    print("After listen")
    await asyncio.sleep(60)

start_async_session("127.0.0.1", 1108, test_callback)

#client.start_listen_blocking("users", "Insert", lambda x: print(x))

#print(client.send_request(create_search_query("FindOne", "unionTest", "matchingKey", "Alex")))

#print(dumps(create_search_query("FindOne", "table", "column", "key")))
