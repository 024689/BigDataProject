import asyncio
import websockets
import json

root_folder = "raw/all"


async def connect_to_websocket():
    uri = "wss://wss.allsportsapi.com/live_events"

    async with websockets.connect(uri) as websocket:
        print("Connected to WebSocket")

        while True:
            try:
                data = await websocket.recv()
                parsed_data = json.loads(data)
                print("Received data:", parsed_data)
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed. Reconnecting...")
                break


def launch_websocket():
    asyncio.get_event_loop().run_until_complete(connect_to_websocket())
