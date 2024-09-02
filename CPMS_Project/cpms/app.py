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
    global central_systems
    logging.info(f"New WebSocket connection with cp_id: {cp_id}")

    try:
        # Initialize CentralSystem for this connection
        central_system = CentralSystem(supabase, cp_id, websocket._get_current_object())
        central_systems[cp_id] = central_system
        await central_system.handle_message()
    except Exception as e:
        logging.error(f"Unexpected error: {e}")


@app.route('/start_transaction/<cp_id>', methods=['POST'])
async def start_transaction(cp_id):
    data = await request.json
    id_tag = data.get('id_tag')
    meter = data.get('meter')
    timestamp = data.get('timestamp')

    if not id_tag:
        return jsonify({"error": "Missing required parameters"}), 400

    try:
        central_system = central_systems.get(cp_id)
        if not central_system:
            logging.error(f"CentralSystem instance for cp_id {cp_id} not found.")
            raise ValueError("CentralSystem instance is not initialized")

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

    try:
        central_system = central_systems.get(cp_id)
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
    try:
        central_system = central_systems.get(cp_id)
        if not central_system:
            raise ValueError("CentralSystem instance is not initialized")

        await central_system.on_get_configuration()
        return jsonify({"status": "Get configuration received"}), 200
    except Exception as e:
        logging.error(f"Error calling get configuration: {e}")
        return jsonify({"error": str(e)}), 500


async def start_servers():
    # Initialize WebSocket server
    logging.info(f"WebSocket server started on ws://0.0.0.0:{port}")
    api_server = asyncio.create_task(app.run_task(host='0.0.0.0', port=port))

    await api_server

if __name__ == "__main__":
    asyncio.run(start_servers())
