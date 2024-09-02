import asyncio
import json
import logging
from quart import Quart, request, jsonify, websocket
from websockets.exceptions import ConnectionClosedError
from supabase import create_client, Client
import websockets
from central_system import CentralSystem
import os
from dotenv import load_dotenv
import uuid

load_dotenv()

app = Quart(__name__)
port = int(os.getenv('PORT', 5000))


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

logging.basicConfig(level=logging.INFO)


def create_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL and key must be set.")
    logging.info(f"Creating Supabase client with URL: {SUPABASE_URL}")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


supabase = create_supabase_client()
central_systems = {}


@app.websocket('/ws/<cp_id>')
async def on_connect(cp_id):
    charge_point_id = str(cp_id).upper()
    global central_systems
    logging.info(f"New WebSocket connection with cp_id: {cp_id}")

    try:
        if charge_point_id in central_systems:
            logging.warning(f"WebSocket connection already exists for cp_id: {cp_id}")
            return

        # Initialize CentralSystem for this connection
        central_system = CentralSystem(supabase, charge_point_id, websocket._get_current_object())
        central_systems[charge_point_id] = central_system
        logging.info(f"CentralSystem instance created and stored for cp_id: {charge_point_id}")
        await central_system.handle_message()
    except Exception as e:
        logging.error(f"Unexpected error during WebSocket connection for cp_id {charge_point_id}: {e}")
    finally:
        # Cleanup the connection
        logging.info(f"WebSocket connection with cp_id {cp_id} closed. Removing from central_systems.")
        central_systems.pop(charge_point_id, None)

@app.route('/start_transaction/<cp_id>', methods=['POST'])
async def start_transaction(cp_id):
    global central_systems
    charge_point_id = str(cp_id).upper()
    data = await request.json
    id_tag = data.get('id_tag')
    meter = data.get('meter')
    timestamp = data.get('timestamp')

    if not id_tag:
        return jsonify({"error": "Missing required parameters"}), 400
    try:
        logging.info(f"Looking for CentralSystem instance for cp_id: {cp_id}")
        central_system = central_systems.get(charge_point_id)

        if not central_system:
            logging.error(f"CentralSystem instance for cp_id {cp_id} not found.")
            return jsonify({"error": "CentralSystem instance is not initialized"}), 500

        await central_system.on_start_transaction(
            id_tag=id_tag,
            meter_start=meter,
            timestamp=timestamp,
        )
        return jsonify({"status": "Start transaction initiated"}), 200

    except Exception as e:
        logging.error(f"Error initiating start transaction for cp_id {cp_id}: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route('/stop_transaction/<cp_id>', methods=['POST'])
async def stop_transaction(cp_id):
    global central_systems
    data = await request.json
    transaction_id = data.get('transaction_id')
    reason = data.get('reason')
    charge_point_id = str(cp_id).upper()

    if not transaction_id:
        return jsonify({"error": "Missing transaction_id"}), 400

    try:
        central_system = central_systems.get(charge_point_id)
        if not central_system:
            raise ValueError("CentralSystem instance is not initialized")

        await central_system.on_stop_transaction(
            transaction_id=transaction_id,
            reason=reason
        )
        return jsonify({"status": "Stop transaction initiated"}), 200
    except Exception as e:
        logging.error(f"Error initiating stop transaction: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/get_config/<cp_id>", methods=['GET'])
async def get_configuration(cp_id):
    charge_point_id = str(cp_id).upper()

    try:
        central_system = central_systems.get(charge_point_id)
        if not central_system:
            raise ValueError("CentralSystem instance is not initialized")

        await central_system.on_get_configuration()
        return jsonify({"status": "Get configuration received"}), 200
    except Exception as e:
        logging.error(f"Error calling get configuration: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/change_config/<cp_id>', methods=['GET'])
async def change_configuration(cp_id):
    global central_systems
    charge_point_id = str(cp_id).upper()
    data = await request.json
    key = data.get('key')
    value = data.get('value')

    if not key or value is None:
        return jsonify({"error": "Missing required parameters"}), 400

    try:
        central_system = central_systems.get(charge_point_id)

        if not central_system:
            logging.error(f"CentralSystem instance for cp_id {cp_id} not found.")
            return jsonify({"error": "CentralSystem instance is not initialized"}), 500
        await central_system.change_configuration(key=key, value=value)
        return jsonify({"status": "Configuration change initiated"}), 200

    except Exception as e:
        logging.error(f"Error changing configuration: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

async def start_servers():
    # Initialize WebSocket server
    logging.info(f"WebSocket server started on ws://0.0.0.0:{port}")
    api_server = asyncio.create_task(app.run_task(host='0.0.0.0', port=port))

    await api_server

if __name__ == "__main__":
    asyncio.run(start_servers())
