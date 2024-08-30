import json

from ocpp.routing import on
from ocpp.v16 import call_result, call, ChargePoint as CP
from ocpp.v16.enums import Action, RegistrationStatus, AuthorizationStatus, RemoteStartStopStatus, DataTransferStatus
from ocpp.v16.datatypes import IdTagInfo
from datetime import datetime
import logging
from supabase import Client
from websockets import ConnectionClosedError

from database import update_charge_point_status, insert_record, insert_diagnostic, fetch_charge_point, \
    generate_transaction_id, fetch_user_by_username_or_email, fetch_charging_session, fetch_transaction, update_record, \
    fetch_charge_point_by_id, update_transaction_status, update_charging_session_status
import asyncio


async def _executeAsyncAfterDelay(coroutine, delaySeconds):
    await asyncio.sleep(delaySeconds)
    await coroutine


def executeAsyncAfterDelay(coroutine, delaySeconds):
    return asyncio.get_event_loop().create_task(_executeAsyncAfterDelay(coroutine, delaySeconds))

class CentralSystem(CP):
    def __init__(self, supabase: Client, charge_point_id, websocket, heartbeat_timeout=120):
        super().__init__(connection=websocket, response_timeout=heartbeat_timeout, id=charge_point_id)
        self.charge_point_last_heartbeat = {}
        self.supabase = supabase
        self.websocket = websocket


    async def handle_message(self):
        try:
            while True:
                message = await self.websocket.receive()
                if message is None:  # Check if the message is None
                    break
                await self.route_message(message)  # Route the message to the appropriate handler
        except Exception as e:
            logging.error(f"Error in handle_message: {e}")
        finally:
            await self.on_disconnect(self.websocket)  # Call on_disconnect when the loop is done

    async def on_disconnect(self, websocket):
        """Handle the disconnection of the WebSocket connection."""
        logging.info(f"Charge Point {self.id} disconnected.")
        insert_diagnostic(self.supabase, self.id, "Disconnect", f"Charge Point {self.id} disconnected.")

        # Update the charge point status to 'offline' in the database
        try:
            update_charge_point_status(self.supabase, self.id, "offline")
            logging.info(f"Charge Point {self.id} status updated to offline.")
            insert_diagnostic(self.supabase, self.id, "Disconnect",
                              f"Charge Point {self.id} status updated to offline.")
        except Exception as e:
            logging.error(f"Failed to update charge point status to offline: {e}")
            insert_diagnostic(self.supabase, self.id, "Error", f"Failed to update charge point status to offline: {e}")

        try:
            self.charge_point_last_heartbeat.pop(self.id, None)
        except Exception as e:
            logging.error(f"Error during cleanup after disconnection: {e}")
            insert_diagnostic(self.supabase, self.id, "Error", f"Error during cleanup after disconnection: {e}")



    @on(Action.Authorize)
    async def on_authorize(self, id_tag):
        """Handle the Authorize request from the charge point."""
        logging.info(f"Authorize request received for ID Tag: {id_tag}")
        insert_diagnostic(self.supabase, self.id, "Authorization", f"Authorization request received for ID tag {id_tag}")
        user_id = fetch_user_by_username_or_email(self.supabase, id_tag)
        if user_id:
            logging.info(f"ID Tag {id_tag} is valid. Authorization accepted.")
            insert_diagnostic(self.supabase, self.id,"Authorization" ,f"ID Tag {id_tag} is valid. Authorization accepted.")
            id_tag_info = IdTagInfo(
                status=AuthorizationStatus.accepted
            )
        else:
            logging.warning(f"ID Tag {id_tag} is invalid. Authorization rejected.")
            insert_diagnostic(self.supabase, self.id, f"ID Tag {id_tag} is invalid. Authorization rejected.")
            id_tag_info = IdTagInfo(
                status=AuthorizationStatus.invalid
            )

        return call_result.Authorize(id_tag_info)

    @on(Action.BootNotification)
    async def on_boot_notification(self, charge_point_serial_number, firmware_version, charge_point_vendor,
                                   charge_point_model, reason=None):
        """Handle the BootNotification request from the charge point."""
        logging.info(
            f"BootNotification received from Charge Point Vendor: {charge_point_vendor}, Model: {charge_point_model}")
        insert_diagnostic(self.supabase, self.id, "BootNotification",
                          f"Boot Notification received from Charge Point Vendor: {charge_point_vendor}, Model: {charge_point_model}")

        cp_data = {
            'status': "available",
            "id": self.id,
            "vendor": charge_point_vendor,
            'model': charge_point_model,
            'serial_number': charge_point_serial_number,
            'firmware_version': firmware_version,
        }

        try:
            # Check if the charge point already exists by serial number or ID
            charge_point_id = fetch_charge_point(self.supabase, charge_point_serial_number=charge_point_serial_number)
            if not charge_point_id:
                charge_point_id = fetch_charge_point_by_id(self.supabase, self.id)

            if not charge_point_id:
                # Insert a new record if the charge point does not exist
                insert_record(self.supabase, "charge_points", cp_data)
                insert_diagnostic(self.supabase, self.id, "Charge Point Inserted",
                                  f"Inserted charge point {self.id} in database.")
            else:
                # Log that the charge point already exists
                logging.info(f"Charge point {self.id} already exists in the database.")
        except Exception as e:
            logging.error(f"Failed to fetch or insert charge point in database: {e}")
            insert_diagnostic(self.supabase, self.id, "Exception",
                              f"Failed to fetch or insert charge point in database: {e}")

        data = {
            "charge_point_id": self.id,
            "reason": reason
        }
        try:
            # Insert the boot notification data
            insert_record(self.supabase, "boot_notifications", data)
            logging.info("BootNotification data inserted into database")
            insert_diagnostic(self.supabase, self.id, "BootNotification", "BootNotification inserted into the database")
        except Exception as e:
            logging.error(f"Failed to insert BootNotification data into database: {e}")
            insert_diagnostic(self.supabase, self.id, "Exception",
                              f"Failed to insert BootNotification into the database: {e}")

        return call_result.BootNotification(
            status=RegistrationStatus.accepted,
            current_time=datetime.utcnow().isoformat() + "Z",
            interval=10
        )

    @on(Action.Heartbeat)
    async def on_heartbeat(self):
        """Handle the heartbeat request from the charge point."""
        logging.info(f"Heartbeat received from Charge Point Id : {self.id}")
        insert_diagnostic(self.supabase, self.id, "Heartbeat", f"Heartbeat received from Charge Point id: {self.id}")
        self.charge_point_last_heartbeat[self.id] = datetime.utcnow()
        try:
            update_charge_point_status(self.supabase, self.id, "online")
            logging.info(f"Charge Point {self.id} status updated to online.")
            insert_diagnostic(self.supabase, self.id, "Heartbeat", f"Charge point {self.id} status updated to online.")
        except Exception as e:
            logging.error(f"Failed to update the charge point status: {e}")
            insert_diagnostic(self.supabase, self.id, "Error", f"Failed to update the charge point status: {e}")
        return call_result.Heartbeat(current_time=datetime.utcnow().isoformat() + "Z")

    @on(Action.MeterValues)
    async def on_meter_values(self, charging_session_id, transaction_id, value):
        """Handle the MeterValues request from the charge point."""
        logging.info(
            f"MeterValues received: Charging session {charging_session_id}, transaction id : {transaction_id} ,value: {value}")
        insert_diagnostic(self.supabase, self.id, "MeterValues",
                          f"MeterValues received: Charging session {charging_session_id}, transaction id : {transaction_id} ,value: {value}")
        data = {
            "charging_session_id": charging_session_id,
            "transaction_id": transaction_id,
            "value": value,
            "unit": "kwH",
            "timestamp": datetime.utcnow()
        }

        try:
            insert_record(self.supabase, "meter_values", data)
            insert_diagnostic(self.supabase, self.id, "MeterValues", "MeterValues data inserted into database.")
            logging.info("MeterValues data inserted into database")
        except Exception as e:
            logging.error(f"Failed to insert MeterValues data into database: {e}")
            insert_diagnostic(self.supabase, self.id, "Exception",
                              f"Failed to insert MeterValues data into database: {e}")

        return call_result.MeterValues()


    async def on_get_configuration(self, **kwargs):
        logging.info("Received the get configuration message.")
        try:
            response = await self.call(call.GetConfiguration())
            logging.info(f"Received GetConfiguration response: {response}")

        except asyncio.TimeoutError:
            logging.error("Timeout while waiting for GetConfiguration response.")

        except Exception as e:
            logging.error(f"Error occurred while handling GetConfiguration: {e}")

    @on(Action.StartTransaction)
    async def on_start_transaction(self, id_tag, meter_start, timestamp, **kwargs):
        """Handle the StartTransaction request from the charge point."""
        logging.info(
            f"StartTransaction received: id_tag={id_tag}, charge point {self.id} ")
        insert_diagnostic(self.supabase, self.id, "StartTransaction", f"StartTransaction received: id_tag={id_tag}")
        logging.info(f"Inserted diagnostic for start transaction.")
        user_id = fetch_user_by_username_or_email(self.supabase, id_tag)
        # Create Charging session
        session_data = {
            "charge_point_id": self.id,
            "user_id": user_id['id'],
            "start_time": timestamp,
            "status": "Ongoing",
        }
        response = insert_record(self.supabase, "charging_sessions", session_data)
        session_id = response['id']
        insert_diagnostic(self.supabase, self.id, "Charging Session", f"Inserted charging session into the database")
        logging.info(f"Charging session inserted into database.")
        transaction_id = generate_transaction_id(self.supabase)
        try:
            transaction_data = {
                'id': transaction_id,
                "charging_session_id": session_id,
                "amount": meter_start,
                "currency": "USD",
                "transaction_time": timestamp,
                "payment_method": "RFID",
                "status": "Started",
            }

            insert_record(self.supabase, "transactions", transaction_data)
            insert_diagnostic(self.supabase, self.id, "Transaction", "Transaction started. Transaction data inserted into database.")
            logging.info("Transaction data inserted into database")
        except Exception as e:
            logging.error(f"Failed to insert transaction data into database: {e}")
            insert_diagnostic(self.supabase, self.id, "Exception", f"Failed to insert transaction data into database")

        id_tag_info = IdTagInfo(
            status=AuthorizationStatus.accepted,
            expiry_date=None,
            parent_id_tag=None
        )
        return call_result.StartTransaction(
            id_tag_info=id_tag_info,
            transaction_id=transaction_id,
        )

    @on(Action.StatusNotification)
    async def on_status_notification(self, connector_id, error_code, status, **kwargs):
        """Handle the StatusNotification request from the charge point."""
        status = status or "Unknown"
        error_code = error_code or "Unknown"
        logging.info(
            f"StatusNotification received: connector_id={connector_id}, status={status}, error_code={error_code}")
        insert_diagnostic(self.supabase, self.id, "StatusNotification",
                          f"StatusNotification received: connector_id={connector_id}, status={status}, error_code={error_code}")
        # await executeAsyncAfterDelay(self.on_get_configuration(), 1)

        try:
            update_charge_point_status(self.supabase, self.id, status)
            logging.info(f"Charge Point {self.id} status updated to {status}.")
            insert_diagnostic(self.supabase, self.id, "StatusNotification",
                              f"Charge Point {self.id} status updated to {status}.")
        except Exception as e:
            logging.error(f"Failed to update the charge point status: {e}")
            insert_diagnostic(self.supabase, self.id, "Error", f"Failed to update the charge point status: {e}")

        return call_result.StatusNotification()

    async def send_remote_start_transaction(self, id_tag):
        """Send a RemoteStartTransaction request to the charge point."""
        request = call.RemoteStartTransaction(
            id_tag=id_tag,
        )

        logging.info(f"Sending RemoteStartTransaction request.")
        insert_diagnostic(self.supabase, self.id, "RemoteStartTransaction", "Sending RemoteStartTransaction request.")
        try:
            response = await self.call(request)
            if response['status'] == RemoteStartStopStatus.accepted:
                logging.info(f"RemoteStartTransaction accepted.")
                insert_diagnostic(self.supabase, self.id, "RemoteStartTransaction",
                                        "RemoteStartTransaction accepted.")
            else:
                logging.warning(f"RemoteStartTransaction rejected.")
                insert_diagnostic(self.supabase, self.id, "RemoteStartTransaction",
                                        "RemoteStartTransaction rejected.")
        except Exception as e:
            logging.error(f"Failed to send RemoteStartTransaction request: {e}")
            insert_diagnostic(self.supabase, self.id, "Error",
                                    f"Failed to send RemoteStartTransaction request: {e}")

    async def initiate_remote_start_transaction(self, id_tag):
        """This function could be triggered externally, e.g., via an API call to start a transaction remotely."""
        logging.info("Initiating remote transaction message received from central system.")
        insert_diagnostic(self.supabase, self.id, "RemoteStartTransaction", "Initiating remote transaction message received from central system.")
        response = await self.on_authorize(id_tag)
        if response.id_tag_info == AuthorizationStatus.invalid:
            logging.error(f"Authorization invalid for id_tag {id_tag}.")
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)
        await self.send_remote_start_transaction(id_tag)

    @on(Action.StopTransaction)
    async def on_stop_transaction(self, transaction_id, reason, **kwargs):
        logging.info(f"StopTransaction received: transaction_id={transaction_id}, reason={reason}")
        insert_diagnostic(self.supabase, self.id, "StopTransaction", f"StopTransaction received: transaction_id={transaction_id}, reason={reason}")
        try:
            transaction = fetch_transaction(self.supabase, transaction_id)
            if not transaction:
                logging.warning(f"Transaction ID {transaction_id} not found.")
                insert_diagnostic(self.supabase, self.id, f"Transaction ID {transaction_id} not found.")
                return call_result.StopTransaction()

            update_transaction_status(self.supabase, transaction_id, "Stopped")
            update_charging_session_status(self.supabase, transaction[0]['charging_session_id'], "Stopped")
            logging.info(f"StopTransaction processed for transaction ID: {transaction_id}")
            insert_diagnostic(self.supabase, self.id, "StopTransaction", f"StopTransaction processed for transaction ID: {transaction_id}")
            id_tage_info = {"status": "Accepted"}
            return call_result.StopTransaction(id_tag_info= id_tage_info)
        except Exception as e:
            id_tage_info = {"status": "Rejected"}
            logging.error(f"Failed to process StopTransaction: {e}")
            return call_result.StopTransaction(id_tag_info=id_tage_info)

    async def send_remote_stop_transaction(self, transaction_id):
        logging.info(f"Sending RemoteStopTransaction for transaction ID: {transaction_id}")
        insert_diagnostic(self.supabase, self.id, "RemoteStopTransaction", f"Sending RemoteStopTransaction for transaction ID: {transaction_id}")
        try:
            request = call.RemoteStopTransaction(transaction_id)
            response = await self.call(request)
            if response.status == RemoteStartStopStatus.accepted:
                logging.info(f"RemoteStopTransaction accepted.")
                insert_diagnostic(self.supabase, self.id, "RemoteStopTransaction" ,f"RemoteStopTransaction accepted.")
            else:
                logging.warning(f"RemoteStopTransaction rejected.")
                insert_diagnostic(self.supabase, self.id, "RemoteSTopTransaction", f"RemoteStopTransaction rejected.")
            return response
        except Exception as e:
            logging.error(f"Failed to send RemoteStopTransaction request: {e}")
            insert_diagnostic(self.supabase, self.id, "Exception", f"Failed to send Remote Stop Transaction request: {e}")

    async def update_transaction_and_session_status(self, transaction_id):
        """Update the transaction and charging session status in the database."""
        try:
            transaction = fetch_transaction(self.supabase, transaction_id)
            if not transaction:
                raise ValueError(f"Transaction with ID {transaction_id} not found.")

            update_transaction_data = {
                'status': 'Stopped',
                'date_updated': datetime.utcnow().isoformat()
            }
            update_record(self.supabase, "transactions", transaction_id, update_transaction_data)

            charging_session_id = transaction['charging_session_id']
            update_charging_session_data = {
                'status': 'Stopped',
                'date_updated': datetime.utcnow().isoformat()
            }
            update_record(self.supabase, "charging_sessions", charging_session_id, update_charging_session_data)
            insert_diagnostic(self.supabase, self.id, "Update", f"Transaction and charging session {charging_session_id} updated to 'Stopped'.")
            logging.info(f"Transaction and charging session {charging_session_id} updated to 'Stopped'.")
        except Exception as e:
            logging.error(f"Failed to update transaction and session status: {e}")
            insert_diagnostic(self.supabase, self.id, "Exception", f"Failed to update transaction and session status: {e}")

    @on(Action.RemoteStartTransaction)
    async def on_remote_start_transaction(self, id_tag, charging_profile=None):
        logging.info(f"RemoteStartTransaction received: id_tag={id_tag}, charging_profile={charging_profile}")
        insert_diagnostic(self.supabase, self.id, "RemoteStartTransaction", f"RemoteStartTransaction received: id_tag={id_tag}, charging_profile={charging_profile}")
        response = await self.on_authorize(id_tag)
        if response.id_tag_info.status == AuthorizationStatus.invalid:
            logging.error(f"Authorization invalid for id_tag {id_tag}.")
            return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.rejected)

        await self.on_start_transaction(id_tag, meter_start=0, timestamp=datetime.utcnow())

        return call_result.RemoteStartTransaction(status=RemoteStartStopStatus.accepted)

    @on(Action.RemoteStopTransaction)
    async def on_remote_stop_transaction(self, transaction_id, reason=None, **kwargs):
        logging.info(f"RemoteStopTransaction received: transaction_id={transaction_id}, reason={reason}")
        insert_diagnostic(self.supabase, self.id, "RemoteStopTransaction", f"RemoteStopTransaction received: transaction_id={transaction_id}, reason={reason}")
        await self.send_remote_stop_transaction(transaction_id)

        await self.update_transaction_and_session_status(transaction_id)

        return call_result.RemoteStopTransaction(status=RemoteStartStopStatus.accepted)

    @on(Action.DataTransfer)
    async def on_data_transfer(self, vendor_id, message_id, data, **kwargs):
        logging.info(
            f"Received data transfer from charge point id {self.id} and vendor with id {vendor_id}, message id is {message_id}, data : {data}")
        return call_result.DataTransfer(DataTransferStatus.accepted)

    @on(Action.DiagnosticsStatusNotification)
    async def on_diagnostics_status_notification(self, status, **kwargs):
        logging.info(f"Received Diagnostics Status Notification from charge point id {self.id}, and status = {status}")
        insert_diagnostic(self.supabase, self.id, "DiagnosticsStatusNotification", f"Status of diagnostics is {status}")
