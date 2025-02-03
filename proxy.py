import asyncio
import websockets
import zmq
import zmq.asyncio
import json
import os

# Obtener variables de entorno
BITCOIN_ZMQ_ADDRESS = os.getenv("BITCOIN_ZMQ_ADDRESS", "tcp://default-bitcoind:9333")
COUNTERPARTY_ZMQ_ADDRESS = os.getenv("COUNTERPARTY_ZMQ_ADDRESS", "tcp://default-counterparty:4001")
WEBSOCKET_PORT = int(os.getenv("WEBSOCKET_PORT", 8765))

# Función para recibir mensajes de ZeroMQ
async def zmq_listener(socket):
    while True:
        try:
            msg = await socket.recv_multipart()
            print(f"[ZMQ] Recibido mensaje: {msg}")
            yield msg
        except Exception as e:
            print(f"[ZMQ] Error en recepción de mensajes: {e}")
            await asyncio.sleep(1)  # Evitar consumo excesivo de CPU en caso de error

# Función para manejar la conexión WebSocket y enviar mensajes desde ZMQ
async def zmq_listener_task(websocket):
    print(f"[WebSocket] Iniciando escucha de ZMQ para {websocket.remote_address}")

    context = zmq.asyncio.Context()
    socket = context.socket(zmq.SUB)
    socket.setsockopt(zmq.RCVHWM, 0)
    socket.connect(COUNTERPARTY_ZMQ_ADDRESS)
    socket.setsockopt_string(zmq.SUBSCRIBE, '')

    async for msg in zmq_listener(socket):
        try:
            json_str = msg[1].decode('utf-8')
            json_obj = json.loads(json_str)
            print(f"[WebSocket] Enviando mensaje: {json_obj}")
            await websocket.send(json.dumps(json_obj))
        except (IndexError, UnicodeDecodeError, json.JSONDecodeError) as e:
            print(f"[WebSocket] Error procesando mensaje: {e}")
            await websocket.send(str(msg))
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"[WebSocket] Conexión cerrada con error: {e}")
            break
        except websockets.exceptions.ConnectionClosedOK as e:
            print(f"[WebSocket] Conexión cerrada normalmente: {e}")
            break

# Función para manejar conexiones WebSocket
async def ws_handler(websocket, path=None):
    print(f"[WebSocket] Cliente conectado desde {websocket.remote_address}")

    try:
        listener_task = asyncio.create_task(zmq_listener_task(websocket))
        ping_task = asyncio.create_task(ping(websocket))

        done, pending = await asyncio.wait(
            [listener_task, ping_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()
    
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"[WebSocket] Conexión cerrada inesperadamente: {e}")
    except Exception as e:
        print(f"[WebSocket] Error desconocido: {e}")
    finally:
        print(f"[WebSocket] Cliente desconectado {websocket.remote_address}")

# Función para enviar pings periódicos
async def ping(websocket):
    while True:
        await asyncio.sleep(60)
        try:
            print(f"[WebSocket] Enviando ping a {websocket.remote_address}")
            await websocket.send("ping")
        except websockets.exceptions.ConnectionClosed:
            print(f"[WebSocket] Conexión cerrada, deteniendo ping.")
            break

# Función principal
async def main():
    print(f"[Servidor] Iniciando WebSocket en 0.0.0.0:{WEBSOCKET_PORT}")
    async with websockets.serve(ws_handler, "0.0.0.0", WEBSOCKET_PORT):
        print("[Servidor] WebSocket corriendo, esperando conexiones...")
        await asyncio.Future()  # Ejecutar indefinidamente

# Iniciar el servidor WebSocket
if __name__ == "__main__":
    asyncio.run(main())
