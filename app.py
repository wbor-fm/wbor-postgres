"""
Postgres Handler.
- Consumes messages from the RabbitMQ queue to insert SMS data into a Postgres database.
"""

import os
import logging
import json
from datetime import datetime, timezone
import signal
import sys

import psycopg
import pika
import pytz
from flask import Flask
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
APP_PORT = os.getenv("APP_PORT", "3000")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "wbor-rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
POSTGRES_QUEUE = os.getenv("POSTGRES_QUEUE", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "wbor-postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)


class EasternTimeFormatter(logging.Formatter):
    """Custom log formatter to display timestamps in Eastern Time."""

    def formatTime(self, record, datefmt=None):
        eastern = pytz.timezone("America/New_York")
        utc_dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        eastern_dt = utc_dt.astimezone(eastern)
        return eastern_dt.isoformat()


formatter = EasternTimeFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logging.getLogger("werkzeug").setLevel(logging.INFO)

app = Flask(__name__)


class RabbitMQBaseConsumer:
    """Base class for RabbitMQ consumers to handle connection and setup."""

    def __init__(self, queue_name, routing_key, exchange="source_exchange"):
        self.connection = None
        self.channel = None
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.exchange = exchange

    def connect(self):
        """Establish connection and channel to RabbitMQ."""
        logger.debug("Attempting to connect to RabbitMQ...")
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            credentials=credentials,
            client_properties={"connection_name": f"{self.queue_name}_Consumer"},
        )
        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()

    def setup_queues(self):
        """Declare exchanges and queues."""
        self.channel.exchange_declare(
            exchange=self.exchange, exchange_type="topic", durable=True
        )
        self.channel.exchange_declare(
            exchange="dead_letter_exchange", exchange_type="direct", durable=True
        )

        try:
            self.channel.queue_declare(
                queue="dead_letter_queue",
                durable=True,
                arguments={
                    "x-message-ttl": 60000,
                    "x-dead-letter-exchange": "dead_letter_exchange",
                },
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            if "inequivalent arg" in str(e):
                logger.warning(
                    "Queue 'dead_letter_queue' already exists with different attributes."
                    "Skipping redeclaration."
                )
                self.connection = pika.BlockingConnection(
                    self.connection.connection_parameters
                )
                self.channel = self.connection.channel()
            else:
                raise

        self.channel.queue_bind(
            exchange="dead_letter_exchange", queue="dead_letter_queue"
        )

        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={
                "x-message-ttl": 60000,
                "x-dead-letter-exchange": "dead_letter_exchange",
            },
        )
        self.channel.queue_bind(
            exchange=self.exchange, queue=self.queue_name, routing_key=self.routing_key
        )

    def stop(self):
        """Gracefully stop consuming and close connections."""
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logger.info("Connections closed.")


class PrimaryQueueConsumer(RabbitMQBaseConsumer):
    """Consumer for the primary queue."""

    def consume_messages(self):
        """Consume messages from the main queue."""
        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=False
        )
        logger.info("Primary queue consumer ready to consume messages.")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Stopping...")
            self.stop()
        except pika.exceptions.AMQPConnectionError as amqp_error:
            logger.error("AMQP Connection Error: %s", amqp_error)
            self.stop()


# Dead-letter queue consumer class
class DeadLetterQueueConsumer(RabbitMQBaseConsumer):
    """Consumer for the dead-letter queue."""

    def __init__(self):
        super().__init__(queue_name="dead_letter_queue", routing_key="")

    def retry_messages(self):
        """Consume messages from the dead-letter queue and retry them."""

        def retry_callback(ch, method, _properties, body):
            logger.info("Retrying message from dead-letter queue.")
            ch.basic_publish(exchange="", routing_key=POSTGRES_QUEUE, body=body)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=retry_callback, auto_ack=False
        )
        logger.info("Dead-letter queue consumer ready to retry messages.")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt received. Stopping...")
            self.stop()


def insert_message_into_postgres(message):
    """Insert a message into the Postgres database."""
    try:
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        with conn.cursor() as cursor:
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

            # Check if the number of columns matches the number of values
            if len(columns) != len(values):
                raise ValueError(
                    "Mismatch between columns and values: "
                    f"{len(columns)} columns, {len(values)} values"
                )

            # Build the query with dynamic columns
            query = f"""
                INSERT INTO {POSTGRES_TABLE} ({', '.join(columns)})
                VALUES ({', '.join(['%s'] * len(values))})
            """
            cursor.execute(query, values)
            conn.commit()
        logger.info("Inserted message into Postgres.")
    except psycopg.errors.DatabaseError as db_error:
        logger.error("Database error: %s", db_error)
        raise
    finally:
        if conn:
            conn.close()


def callback(ch, method, properties, body):
    """
    Make a connection to Postgres and insert the message into the database.
    """
    logger.info("Callback triggered.")

    retry_count = 0 # Safeguard against NoneType for headers
    if properties and properties.headers:
        retry_count = properties.headers.get("x-retry-count", 0)

    try:
        message = json.loads(body)
        logger.debug("Processing message: %s", message)

        # Connect to Postgres and insert the message
        insert_message_into_postgres(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except psycopg.errors.DatabaseError as db_error:
        logger.error("Error processing message: %s", db_error)
        retry_message(ch, method, body, retry_count)
    except (json.JSONDecodeError, KeyError) as e:
        logger.error("Failed to process message: %s", e)
        retry_message(ch, method, body, retry_count)


def retry_message(ch, method, body, retry_count):
    """Retry or send message to DLQ if retry limit is reached."""
    if retry_count < MAX_RETRIES:
        logger.info("Retrying message. Retry count: %d", retry_count + 1)
        ch.basic_publish(
            exchange="",
            routing_key=POSTGRES_QUEUE,
            body=body,
            properties=pika.BasicProperties(headers={"x-retry-count": retry_count + 1}),
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        logger.warning("Max retries reached! Routing message to DLQ.")
        ch.basic_publish(
            exchange="dead_letter_exchange",
            routing_key="dead_letter_queue",
            body=body,
            properties=pika.BasicProperties(headers={"x-retry-count": retry_count}),
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)


def signal_handler(consumers, sig, _):
    """
    Gracefully stop consumers on SIGINT or SIGTERM.
    """
    logger.info("Signal received: %s. Shutting down consumers gracefully...", sig)
    for consumer in consumers:
        consumer.stop()
    sys.exit(0)


# Register signal handlers
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@app.route("/")
def hello_world():
    """Serve a simple static Hello World page at the root"""
    return "<h1>wbor-postgres-driver is online!</h1>"


if __name__ == "__main__":
    # Only start the Flask app; Gunicorn handles consumer initialization
    app.run(host="0.0.0.0", port=3000)
