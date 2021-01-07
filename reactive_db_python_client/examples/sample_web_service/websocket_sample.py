import asyncio
from reactive_db_client.communication_types import create_insert_request
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from reactive_db_client import ClientSync, create_search_query, ClientAsync, start_async_session

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, async_mode="threading")
rdb_client = ClientSync("127.0.0.1", 1108)
rdb_client.connect()
@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('add_user', namespace='/test')
def test_message(message):
    query = create_insert_request({"name": message["name"], "age": int(message["age"])}, "users")
    response = rdb_client.send_request(query)
    emit('my_response', {'data': "Added user: "+message["name"]})

@socketio.on('add_grade', namespace='/test')
def test_message(message):
    query = create_insert_request({"name": message["name"], "grade": int(message["grade"])}, "grades")
    response = rdb_client.send_request(query)
    emit('my_response', {'data': "Added grade for user: "+message["name"]})

@socketio.on('my_broadcast_event', namespace='/test')
def test_message(message):
    print(message)
    emit('my_response', {'data': message['data']}, broadcast=True)

@socketio.on('connect', namespace='/test')
def test_connect():
    emit('my_response', {'data': 'Connected'})
    #background()


@socketio.on('disconnect', namespace='/test')
def test_disconnect():
    print('Client disconnected')

def background():
    def callback(entry):
        print(entry["Event"]["value"]["OneResult"]["Ok"])
        socketio.emit('my_response', {'data': "average grade for user "+ entry["Event"]["value"]["OneResult"]["Ok"]["aggregatedColumn"]["Str"] +" is now: "+str(entry["Event"]["value"]["OneResult"]["Ok"]["average"]["Decimal"])}, broadcast = True, namespace="/test")
    second_client = ClientSync("127.0.0.1", 1108)
    second_client.connect()
    second_client.start_listen_blocking("aggregationTest", "Insert", callback)
    '''async def session(client: ClientAsync):
        print("Starting listen")
        await client.start_listen_blocking("aggregationTest", "Insert", callback)
        #await asyncio.sleep(1000000)
    print("Background Task")'''

    #start_async_session("127.0.0.1", 1108, session)

socketio.start_background_task(background)

if __name__ == '__main__':
    socketio.run(app)
