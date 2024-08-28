import asyncio
import json
import logging
from quart import Quart, request, jsonify
from websockets.exceptions import ConnectionClosedError
from supabase import create_client, Client
import websockets
from central_system import CentralSystem
import os
from dotenv import load_dotenv

load_dotenv()

app = Quart(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logging.basicConfig(level=logging.INFO)

def create_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL and key must be set.")
    logging.info(f"Creating Supabase client with URL: {SUPABASE_URL}")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = create_supabase_client()

async def route_message(central_system, message):
    try:
        msg = json.loads(message)
        action = msg[2]

        if action == "BootNotification":
            await central_system.on_boot_notification(*msg[3:])
        elif action == "Authorize":
            await central_system.on_authorize(*msg[3:])
        elif action == "Heartbeat":
            await central_system.on_heartbeat()
        elif action in ["StartTransaction", "StartCharging"]:
            await central_system.on_start_transaction(*msg[3:])
        elif action == "StopTransaction":
            await central_system.on_stop_transaction(*msg[3:])
        elif action == "RemoteStartTransaction":
            await central_system.on_remote_start_transaction(*msg[3:])
        elif action == "RemoteStopTransaction":
            await central_system.on_remote_stop_transaction(*msg[3:])
        elif action == "MeterValues":
            await central_system.on_meter_values(*msg[3:])
        elif action == "StatusNotification":
            await central_system.on_status_notification(*msg[3:])
        else:
            logging.warning(f"Unhandled message action: {action}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse message: {e}")
    except Exception as e:
        logging.error(f"Error handling message: {e}")

@app.websocket('/ws/<cp_id>')
async def on_connect(cp_id):
    cp_id = cp_id
    logging.info(f"New WebSocket connection with cp_id: {cp_id}")

    try:
        central_system = CentralSystem(supabase, cp_id, websocket._get_current_object())
        async for message in websocket:
            logging.info(f"Received message: {message}")
            await route_message(central_system, message)
    except ConnectionClosedError as e:
        logging.error(f"Connection closed error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        logging.info(f"Connection with cp_id {cp_id} closed.")

@app.route('/start_transaction/<cp_id>', methods=['POST'])
async def start_transaction(cp_id):
    data = await request.json
    id_tag = data.get('id_tag')
    meter = data.get('meter')
    timestamp = data.get('timestamp')

    if not id_tag:
        return jsonify({"error": "Missing required parameters"}), 400

    central_system = CentralSystem(supabase=supabase, charge_point_id=cp_id)
    try:
        await central_system.on_start_transaction(
            id_tag=id_tag,
            meter_start=meter,
            timestamp=timestamp,
        )
        return jsonify({"status": "Start transaction initiated"}), 200
    except Exception as e:
        logging.error(f"Error initiating start transaction: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/stop_transaction/<cp_id>', methods=['POST'])
async def stop_transaction(cp_id):
    data = await request.json
    transaction_id = data.get('transaction_id')
    reason = data.get('reason')

    if not transaction_id:
        return jsonify({"error": "Missing transaction_id"}), 400

    central_system = CentralSystem(supabase, cp_id)
    try:
        await central_system.on_stop_transaction(
            transaction_id=transaction_id,
            reason=reason
        )
        return jsonify({"status": "Stop transaction initiated"}), 200
    except Exception as e:
        logging.error(f"Error initiating stop transaction: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/get_config/<cp_id>")
async def get_configuration(cp_id):
    central_system = CentralSystem(supabase, cp_id)
    try:
        await central_system.on_get_configuration()
        return jsonify({"status": "get configuration received"}), 200
    except Exception as e:
        logging.error("Error calling get config")
        return jsonify({"error": str(e)}), 500

async def start_servers():
    ws_server = await websockets.serve(on_connect, "0.0.0.0", 8000)
    logging.info("WebSocket server started on ws://0.0.0.0:8000")

    # Run the Quart app concurrently
    api_server = asyncio.create_task(app.run_task(host='0.0.0.0', port=5000))

    await ws_server.wait_closed()
    await api_server

if __name__ == "__main__":
    asyncio.run(start_servers())
