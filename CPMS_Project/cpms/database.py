import logging
from datetime import datetime
from supabase import create_client, Client
import os


def init_supabase() -> Client:
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Supabase URL and Key must be set")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def insert_record(supabase: Client, table: str, data: dict):
    response = supabase.from_(table).insert(data).execute()
    if not response.data:
        raise Exception(f"Failed to insert data into {table}: {response.error}")
    return response.data[0]


def fetch_records(supabase: Client, table: str):
    response = supabase.from_(table).select("*").execute()
    if not response.data:
        raise Exception(f"Failed to fetch data from {table}: {response.error}")
    return response.data


def update_record(supabase: Client, table: str, record_id: str, data: dict):
    response = supabase.from_(table).update(data).eq("id", record_id).execute()
    if not response.data:
        raise Exception(f"Failed to update data from {table}: {response.error}")
    return response.data


def delete_record(supabase: Client, table: str, record_id: str):
    response = supabase.from_(table).delete().eq("id", record_id).execute()
    if not response.data:
        raise Exception(f"Failed to delete data from {table}: {response.error}")
    return response.data


def fetch_boot_notifications_by_vendor(supabase: Client, vendor: str):
    response = supabase.from_("boot_notifications").select("*").eq("charge_point_vendor", vendor).execute()
    if not response.data:
        raise Exception(f"Failed to fetch data: {response.error}")
    return response.data


def update_charge_point_status(supabase:Client, charge_point_id, status):
    response = supabase.from_("charge_points").update({"status": status}).eq("id", charge_point_id).execute()
    if not response:
        raise Exception(f"Failed to update the charge point status : {response.error}")
    return response.data


def fetch_charge_point(supabase, charge_point_serial_number):
    """Fetch a charge point by its ID from the charge_points table."""
    response = supabase.from_("charge_points").select("*").eq("serial_number", charge_point_serial_number).execute()
    if response.data and len(response.data) > 0:
        logging.info(f"Charge point with serial number {charge_point_serial_number}")
        return response.data[0]
    else:
        logging.info(f"Charge point with serial number {charge_point_serial_number} doesn't exist in database.")
        return None


def fetch_firmware(supabase, firmware_id: str):
    """Fetch firmware by its ID from the firmware table."""
    response = supabase.from_("firmware").select("*").eq("id", firmware_id).execute()
    if response.error:
        raise Exception(f"Failed to fetch firmware: {response.error}")
    return response.data


def fetch_user(supabase, user_id: str):
    """Fetch a user by its ID from the users table."""
    response = supabase.from_("users").select("*").eq("id", user_id).execute()
    if response.error:
        raise Exception(f"Failed to fetch user: {response.error}")
    return response.data


def check_charge_point_availability(supabase, charge_point_id):
    """Check if the charge point is available for a new transaction."""
    try:
        response = supabase.from_("charge_points").select("status").eq("id", charge_point_id).execute()
        if response.error or not response.data:
            logging.error(f"Failed to fetch charge point status: {response.error}")
            return "unavailable"
        charge_point_status = response.data[0]["status"]
        if charge_point_status == "available":
            session_response = supabase.from_("charging_sessions").select("*").eq("charge_point_id", charge_point_id).eq("status", "active").execute()
            if session_response.data:
                logging.warning(f"Charging point {charge_point_id} is currently engaged in an active session.")
                return "occupied"
            logging.info(f"Charge point { charge_point_id} is available.")
            return "available"
        else:
            logging.warning(f"Charge point {charge_point_id} is unavailable: {charge_point_status}")
            return charge_point_status
    except Exception as e:
        logging.error(f"Error checking charge point availability: {e}")
        return "unavailable"





def insert_diagnostic(supabase, charge_point_id, diagnostic_type, message, details=None):
    """Insert a diagnostic record into the database."""
    diagnostic_data = {
        "charge_point_id": charge_point_id,
        "timestamp": datetime.now().isoformat(),
        "error_code": diagnostic_type,
        "message": message,
    }
    try:
        response = supabase.from_("diagnostics").insert(diagnostic_data).execute()
        logging.info(f"Inserted diagnostic entry for charge point {charge_point_id}: {message}")
        return response.data
    except Exception as e:
        logging.error(f"Failed to insert diagnostic entry: {e}")


def fetch_charging_session(supabase: Client, charging_session_id: str):
    """Fetch a charging session by its ID from the charging_sessions table."""
    response = supabase.from_("charging_sessions").select("*").eq("id", charging_session_id).execute()
    if not response:
        raise Exception(f"No charging session found with ID {charging_session_id}")
    return response.data


def fetch_transaction(supabase: Client, id):
    """Fetch a transaction by the time from Transactions table."""
    response = supabase.from_("transactions").select("*").eq("id", id)   .execute()
    if not response:
        raise Exception(f"No transaction found at time {id}")
    return response.data


def update_charging_session_status(supabase: Client, charging_session_id: str, status: str):
    """Update the status of a charging session in the charging_sessions table."""
    response = supabase.from_("charging_sessions").update({"status": status}).eq("id", charging_session_id).execute()
    if not response:
        raise Exception(f"Failed to update charging session status.")
    return response.data


def update_transaction_status(supabase: Client, transaction_id: str, status: str):
    """Update the status of a charging session in the charging_sessions table."""
    response = supabase.from_("transactions").update({"status": status}).eq("id", transaction_id).execute()
    if not response:
        raise Exception(f"Failed to update transaction status.")
    return response.data


def fetch_user_by_username_or_email(supabase, id_token: str):
    try:
        response = supabase.from_("users").select("*").eq("username", id_token).execute()
        if response.data:
            return response.data[0]

        response = supabase.from_("users").select("*").eq("email", id_token).execute()
        if response.data:
            return response.data[0]

        return None
    except Exception as e:
        logging.error(f"Error fetching user: {e}")
        return None


def generate_transaction_id(supabase):
    """Generate the next transaction ID based on the last ID in the transactions table."""
    query = supabase.table('transactions').select('id').order('id', desc=True).limit(1).execute()

    if query.data:
        last_transaction_id = query.data[0]['id']
        return last_transaction_id + 1
    else:
        return 1


def fetch_connector_status(supabase, charge_point_id, connector_id):
    """
    Fetch the status of a connector from the database.
    """
    try:
        response = supabase.table('charge_points').select('status').eq('id', charge_point_id).single()
        connectors = response.get('status', {})
        return connectors.get(connector_id, 'unknown')
    except Exception as e:
        logging.error(f"Failed to fetch connector status from database: {e}")
        return 'unknown'


def fetch_charge_point_by_id(supabase, charge_point_id):
    """Fetch charge point."""
    response = supabase.from_("charge_points").select('*').eq('id', charge_point_id).execute()
    if response.data:
        return response.data[0]
    if not response.data:
        return None
    else:
        raise Exception(f"Charge point with id {charge_point_id} not found.")


def update_firmware_status(supabase, firmware_id, status):
    response = supabase.table("firmware").update({"status": status}).eq("version", firmware_id).execute()
    if not response:
        raise Exception(f"Failed to update firmware status.")
    return response.data
