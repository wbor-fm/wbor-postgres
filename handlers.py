"""
Implement business logic for processing messages based on their type or purpose.
"""

from utils.logging import configure_logging
from database import execute_query
from config import MESSAGES_TABLE, GROUPME_TABLE

logger = configure_logging(__name__)

MESSAGE_HANDLERS = {}


def register_message_handler(message_type):
    """Decorator to register a handler for a specific message type."""

    def decorator(func):
        MESSAGE_HANDLERS[message_type] = func
        return func

    return decorator


@register_message_handler("sms")
def handle_twilio_sms(message, cursor):
    """
    Handle insertion of Twilio SMS messages.

    Calls database.execute_query with the appropriate query and values.

    TODO: outbound vs inbound messages
    """
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

    # Build the query with dynamic columns
    query = f"""
        INSERT INTO {MESSAGES_TABLE} ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(values))})
    """
    execute_query(cursor, query, values)

# @register_message_handler("groupme")
# def handle_generic_event(message, routing_key cursor):
#     """Handle insertion of generic event messages."""
#     query = f"""
#         INSERT INTO {GROUPME_TABLE} ("event_id", "event_name", "timestamp")
#         VALUES (%s, %s, %s)
#     """
#     cursor.execute(
#         query,
#         (message.get("event_id"), message.get("event_name"), message.get("timestamp")),
#     )

# Example handler for generic messages
# @register_message_handler("generic_event")
# def handle_generic_event(message, routing_key cursor):
#     """Handle insertion of generic event messages."""
#     query = f"""
#         INSERT INTO {POSTGRES_TABLE} ("event_id", "event_name", "timestamp")
#         VALUES (%s, %s, %s)
#     """
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
#     query = f"INSERT INTO rds_table ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
#     cursor.execute(query, values)
