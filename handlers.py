"""
Implement business logic for processing messages based on their type or purpose.
"""

from datetime import datetime
import json
from utils.logging import configure_logging
from database import build_insert_query, execute_query
from config import MESSAGES_TABLE, GROUPME_TABLE, GROUPME_CALLBACK_TABLE

logger = configure_logging(__name__)

MESSAGE_HANDLERS = {}


def register_message_handler(message_type):
    """Decorator to register a handler for a specific message type."""

    def decorator(func):
        MESSAGE_HANDLERS[message_type] = func
        return func

    return decorator


@register_message_handler("postgres")
def handle_postgres_data(_message, _cursor):
    """
    TODO: Not sure what to do here just yet.

    This handler exists so that the DLQ doesn't infinitely retry messages.

    DLQ uses "postgres" as the message type due to default bindings.
    """
    logger.warning("Received message with type 'postgres'. No handler implemented.")


# def add_to_contacts(message, cursor):
#     """
#     Add a new contact to the contacts table.

#     This function is called by the handle_contact_event function.
#     """
#     columns = ['"phone_number"', '"contact_name"']
#     values = [message.get("phone_number"), message.get("contact_name")]

#     query, values = build_insert_query("contacts", columns, values)
#     cursor.execute(query, values)


# def handle_contact_event(message, cursor):
#     """
#     Handle the insertion of a new contact.
#     """
#     add_to_contacts(message, cursor)


@register_message_handler("twilio.sms.incoming")
def handle_twilio_sms(message, cursor):
    """
    Handle insertion of incoming Twilio SMS messages.

    Calls database.execute_query with the appropriate query and values.

    TODO: outbound vs inbound messages
    """
    logger.debug("Handling twilio.sms.incoming message: %s", message)
    # Prepare additional columns and values for From(LocationType) if they exist
    location_columns = []
    location_values = []
    for loc_type in ["FromCity", "FromState", "FromCountry", "FromZip"]:
        if message.get(loc_type):
            location_columns.append(f'"{loc_type}"')
            location_values.append(message.get(loc_type))

    # Prepare additional columns and values for media items
    media_columns = []
    media_values = []
    for i in range(10):
        media_type_key = f"MediaContentType{i}"
        media_url_key = f"MediaUrl{i}"

        if message.get(media_type_key):
            media_columns.append(f'"MediaContentType{i}"')
            media_values.append(message.get(media_type_key))

        if message.get(media_url_key):
            media_columns.append(f'"MediaUrl{i}"')
            media_values.append(message.get(media_url_key))

    # Combine static columns with dynamic columns
    columns = (
        [
            '"MessageSid"',
            '"AccountSid"',
            '"MessagingServiceSid"',
            '"From"',
            '"To"',
            '"Body"',
            '"NumSegments"',
            '"NumMedia"',
            '"ApiVersion"',
            '"SenderName"',
            '"wbor_message_id"',
        ]
        + location_columns
        + media_columns
    )
    values = (
        [
            message.get("MessageSid"),
            message.get("AccountSid"),
            message.get("MessagingServiceSid"),
            message.get("From"),
            message.get("To"),
            message.get("Body"),
            message.get("NumSegments"),
            message.get("NumMedia"),
            message.get("ApiVersion"),
            message.get("SenderName"),
            message.get("wbor_message_id"),
        ]
        + location_values
        + media_values
    )

    query, values = build_insert_query(MESSAGES_TABLE, columns, values)
    execute_query(cursor, query, values)


@register_message_handler("twilio.sms.outgoing")
def handle_outgoing_twilio_sms(message, _cursor):
    """
    TEMP
    """
    logger.debug("Handling twilio.sms.outgoing message: %s", message)


@register_message_handler("groupme.msg")
def handle_message_event(message, cursor):
    """
    Handle insertion of text message logs from GroupMe.
    """
    logger.debug("Handling groupme.msg message: %s", message)
    # Prepare the columns and values to insert
    columns = [
        '"text"',
        '"bot_id"',
        '"code"',
        '"type"',
        '"uid"',
        '"picture_url"',
        '"source"',
    ]
    values = [
        message.get("text"),
        message.get("bot_id"),
        message.get("code"),
        message.get("type"),
        message.get("uid"),
        message.get("picture_url"),
        message.get("source"),
    ]

    # Build and execute the SQL query
    query, values = build_insert_query(GROUPME_TABLE, columns, values)
    cursor.execute(query, values)


@register_message_handler("groupme.img")
def handle_image_event(message, cursor):
    """
    Handle insertion of image message logs from GroupMe.
    """
    logger.debug("Handling groupme.img message: %s", message)
    # Prepare the columns and values to insert
    columns = [
        '"raw_img"',
        '"bot_id"',
        '"code"',
        '"type"',
        '"uid"',
        '"picture_url"',
        '"text"',
        '"source"',
    ]
    values = [
        message.get("raw_img"),  # Unlikely to be used based on current implementation
        message.get("bot_id"),
        message.get("code"),
        message.get("type"),
        message.get("uid"),
        message.get("picture_url"),
        message.get("text"),
        message.get("source"),
    ]

    query, values = build_insert_query(GROUPME_TABLE, columns, values)
    cursor.execute(query, values)


@register_message_handler("groupme.callback")
def handle_callback_event(message, cursor):
    """
    Handle insertion of callback logs from GroupMe.
    """
    logger.debug("Handling groupme.callback message: %s", message)

    # Convert the Unix timestamp to a datetime object
    created_at = datetime.fromtimestamp(message.get("created_at"))

    # Prepare the columns and values to insert
    columns = [
        '"attachments"',
        '"avatar_url"',
        '"created_at"',
        '"group_id"',
        '"id"',
        '"name"',
        '"sender_id"',
        '"sender_type"',
        '"source_guid"',
        '"system"',
        '"text"',
        '"user_id"',
    ]
    values = [
        json.dumps(message.get("attachments", [])),  # Convert list to JSON string
        message.get("avatar_url"),
        created_at,
        message.get("group_id"),
        message.get("id"),  # Rename 'id' to 'message_id' for clarity
        message.get("name"),
        message.get("sender_id"),
        message.get("sender_type"),
        message.get("source_guid"),
        message.get("system"),
        message.get("text"),
        message.get("user_id"),
    ]

    query, values = build_insert_query(GROUPME_CALLBACK_TABLE, columns, values)
    cursor.execute(query, values)


# Example handler for generic messages
# @register_message_handler("generic_event")
# def handle_generic_event(message, cursor):
#     """Handle insertion of generic event messages."""
#     cursor.execute(
#         query,
#         (message.get("event_id"), message.get("event_name"), message.get("timestamp")),
#     )

# @register_message_handler("rds")
# def handle_rds_data(message, cursor):
#     """Handle Radio Data System (RDS) data messages."""
#     columns = ["song_title", "artist", "timestamp"]
#     values = [
#         message.get("song_title"),
#         message.get("artist"),
#         message.get("timestamp"),
#     ]
#     cursor.execute(query, values)
