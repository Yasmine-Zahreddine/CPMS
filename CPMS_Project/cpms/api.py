from flask import Flask, request, jsonify
import logging
from supabase import create_client, Client
from central_system import CentralSystem
import os
from dotenv import load_dotenv
load_dotenv()


app = Flask(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# Create Supabase client

def create_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL and key must be set.")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


@app.route('/start_transaction/<cp_id>', methods=['POST'])
async def start_transaction(cp_id):
    data = request.json
    charge_point_id = cp_id
    id_tag = data.get('id_tag')
    meter = data.get('meter')
    timestamp = data.get('timestamp')
    if not charge_point_id or not id_tag:
        return jsonify({"error": "Missing required parameters"}), 400

    supabase = create_supabase_client()
    central_system = CentralSystem(supabase, charge_point_id)

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
    data = request.json
    transaction_id = data.get('transaction_id')
    reason = data.get('reason')
    charge_point_id = cp_id
    if not transaction_id:
        return jsonify({"error": "Missing transaction_id"}), 400

    supabase = create_supabase_client()
    central_system = CentralSystem(supabase, charge_point_id)
    try:
        # Send RemoteStopTransaction request
        await central_system.on_stop_transaction(
            transaction_id=transaction_id,
            reason= reason
        )
        return jsonify({"status": "Stop transaction initiated"}), 200
    except Exception as e:
        logging.error(f"Error initiating stop transaction: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
