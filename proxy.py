import asyncio
import websockets
import zmq
import zmq.asyncio
import json
import os

BITCOIN_ZMQ_ADDRESS = os.getenv("BITCOIN_ZMQ_ADDRESS", "tcp://bitcoind:9333")
COUNTERPARTY_ZMQ_ADDRESS = os.getenv("COUNTERPARTY_ZMQ_ADDRESS", "tcp://counterparty-core:4001")
WEBSOCKET_PORT = int(os.getenv("WEBSOCKET_PORT", 8765))

def setup_zmq_socket(context, address, topics):
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.connect(address)
    for topic in topics:
        socket.setsockopt_string(zmq.SUBSCRIBE, topic)
    return socket

async def zmq_listener(socket, websocket):
    while True:
        msg = await socket.recv_multipart()
        try:
            json_str = msg[1].decode('utf-8')
            json_obj = json.loads(json_str)
            print(f"Sending WebSocket message: {json_obj}")
            await websocket.send(json.dumps(json_obj))
        except (IndexError, UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"Error processing message: {e}")
            await websocket.send(str(msg))
        except websockets.exceptions.ConnectionClosed:
            print("WebSocket connection closed")
            break

async def zmq_listener_task(websocket, subscriptions):
    context = zmq.asyncio.Context()
    
    # Mantener sockets compartidos por conexión
    zmq_sockets = {}
    if "bitcoin" in subscriptions:
        zmq_sockets["bitcoin"] = setup_zmq_socket(context, BITCOIN_ZMQ_ADDRESS, [""])
    if "counterparty" in subscriptions:
        zmq_sockets["counterparty"] = setup_zmq_socket(context, COUNTERPARTY_ZMQ_ADDRESS, [""])
    
    tasks = [zmq_listener(socket, websocket) for socket in zmq_sockets.values()]
    await asyncio.gather(*tasks)

async def ws_handler(websocket, path=None):
    # Suscribirse por defecto a Bitcoin y Counterparty
    subscriptions = {"bitcoin", "counterparty"}
    print(f"Subscriptions: {subscriptions}")
    try:
        message = await websocket.recv()
        data = json.loads(message)
        if "subscribe" in data:
            subscriptions = set(data["subscribe"])  # Actualizar suscripciones según el cliente
    except (json.JSONDecodeError, websockets.exceptions.ConnectionClosed):
        pass
    
    listener_task = asyncio.create_task(zmq_listener_task(websocket, subscriptions))
    ping_task = asyncio.create_task(ping(websocket))

    done, pending = await asyncio.wait(
        [listener_task, ping_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in pending:
        task.cancel()

async def ping(websocket):
    while True:
        await asyncio.sleep(60)
        print("Sending ping")
        await websocket.send("ping")

async def main():
    async with websockets.serve(ws_handler, "0.0.0.0", WEBSOCKET_PORT):
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    asyncio.run(main())
