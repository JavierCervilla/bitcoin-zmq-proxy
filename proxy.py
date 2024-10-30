import asyncio
import websockets
import zmq
import zmq.asyncio
import binascii
import json
from dotenv import load_dotenv

load_dotenv()

BITCOIN_ZMQ_ADDRESS = os.getenv("BITCOIN_ZMQ_ADDRESS", "tcp://default-bitcoind:9333")
COUNTERPARTY_ZMQ_ADDRESS = os.getenv("COUNTERPARTY_ZMQ_ADDRESS", "tcp://default-counterparty:4001")
WEBSOCKET_PORT = int(os.getenv("WEBSOCKET_PORT", 8765))

async def zmq_listener(socket):
    while True:
        msg = await socket.recv_multipart()
        print(f"Received ZMQ message: {msg}")
        yield msg

async def zmq_listener_task(websocket):
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.connect(BITCOIN_ZMQ_ADDRESS)
    socket.setsockopt_string(zmq.SUBSCRIBE, 'rawblock')
    socket.connect(COUNTERPARTY_ZMQ_ADDRESS)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')

    async for msg in zmq_listener(socket):
        try:
            json_str = msg[1].decode('utf-8')
            json_obj = json.loads(json_str)
            print(f"Sending WebSocket message: {json_obj}")
            await websocket.send(json.dumps(json_obj))
        except (IndexError, UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"Error processing message: {e}")
            await websocket.send(str(msg))
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"WebSocket connection closed with error: {e}")
            break
        except websockets.exceptions.ConnectionClosedOK as e:
            print(f"WebSocket connection closed normally: {e}")
            break

async def ws_handler(websocket, path):
    while True:
        try:
            listener_task = asyncio.create_task(zmq_listener_task(websocket))
            done, pending = await asyncio.wait(
                [listener_task],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"WebSocket connection closed, retrying: {e}")
            await asyncio.sleep(1)  # Wait a bit before retrying
        except websockets.exceptions.ConnectionClosedOK as e:
            print(f"WebSocket connection closed normally, retrying: {e}")
            await asyncio.sleep(1)  # Wait a bit before retrying

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
