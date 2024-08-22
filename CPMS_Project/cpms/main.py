import asyncio
import json
import logging
from websockets.exceptions import ConnectionClosedError
from supabase import create_client, Client
import websockets
from central_system import CentralSystem
import os
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logging.basicConfig(level=logging.INFO)


async def route_message(self, message):
    try:
        msg = json.loads(message)
        action = msg[2]

        if action == "BootNotification":
            await self.on_boot_notification(*msg[3:])
        elif action == "Authorize":
            await self.on_authorize(*msg[3:])
        elif action == "Heartbeat":
            await self.on_heartbeat()
        elif action == "StartTransaction" or action == "StartCharging":
            await self.on_start_transaction(*msg[3:])
        elif action == "StopTransaction":
            await self.on_stop_transaction(*msg[3:])
        elif action == "RemoteStartTransaction":
            await self.on_remote_start_transaction(*msg[3:])
        elif action == "RemoteStopTransaction":
            await self.on_remote_stop_transaction(*msg[3:])
        elif action == "MeterValues":
            await self.on_meter_values(*msg[3:])
        elif action == "StatusNotification":
            await self.on_status_notification(*msg[3:])
        else:
            logging.warning(f"Unhandled message action: {action}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse message: {e}")
    except Exception as e:
        logging.error(f"Error handling message: {e}")



def create_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL and key must be set.")
    logging.info(f"Creating Supabase client with URL: {SUPABASE_URL}")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


async def on_connect(websocket, path):
    charge_point_id = path.strip('/')
    logging.info(f"New connection with path: {path}")
    supabase = create_supabase_client()

    try:
        central_system = CentralSystem(supabase, charge_point_id, websocket)
        async for message in websocket:
            logging.info(f"Received message: {message}")
            await central_system.route_message(message)

    except ConnectionClosedError as e:
        logging.error(f"Connection closed error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info(f"Connection with {charge_point_id} closed.")


async def main():
    server = await websockets.serve(on_connect, "localhost", 8000)
    logging.info("WebSocket server started on ws://0.0.0.0:8000")

    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
