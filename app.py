"""
Postgres Handler.
- Consumes messages from the RabbitMQ queue to insert SMS data into a Postgres database.
"""

import os
import logging
import json
import threading
from datetime import datetime, timezone
import psycopg
import pika
import pika.exceptions
import pytz
from flask import Flask
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
APP_PORT = os.getenv("APP_PORT", "5000")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "wbor-rabbitmq")
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
POSTGRES_QUEUE = os.getenv("POSTGRES_QUEUE", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "wbor-postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Define a handler to output to the console
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)


class EasternTimeFormatter(logging.Formatter):
    """Custom log formatter to display timestamps in Eastern Time"""

    def formatTime(self, record, datefmt=None):
        # Convert UTC to Eastern Time
        eastern = pytz.timezone("America/New_York")
        utc_dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        eastern_dt = utc_dt.astimezone(eastern)
        # Use ISO 8601 format
        return eastern_dt.isoformat()


formatter = EasternTimeFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
logging.getLogger("werkzeug").setLevel(logging.INFO)

app = Flask(__name__)


def connect_to_postgres():
    """Establish a connection to the Postgres database."""
    try:
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        logger.info("Connected to Postgres database.")
        return conn
    except psycopg.Error as e:
        logger.error("Error connecting to Postgres: %s", e)
        return None


def callback(ch, method, properties, body):
    """Callback function to process messages from the RabbitMQ queue."""
    try:
        message = json.loads(body)
        logger.debug("Received message: %s", message)

        # Process and insert SMS data into Postgres
        phone_number = message.get("phone_number")
        text = message.get("text")
        timestamp = message.get("timestamp")

        conn = connect_to_postgres()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO sms_messages (phone_number, text, timestamp)
                        VALUES (%s, %s, %s)
                        """,
                        (phone_number, text, timestamp),
                    )
                conn.commit()
                logger.info("Inserted message into Postgres.")
            except psycopg.errors.DatabaseError as db_error:
                logger.error("Database error during insertion: %s", db_error)
            finally:
                conn.close()
        else:
            logger.error("No database connection available.")
    except (json.JSONDecodeError, KeyError) as e:
        logger.error("Failed to process message: %s", e)


def consume_messages():
    """Consume messages from the RabbitMQ queue."""
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=POSTGRES_QUEUE, durable=True)
        channel.basic_consume(
            queue=POSTGRES_QUEUE, on_message_callback=callback, auto_ack=True
        )
        logger.info("Started consuming messages.")
        channel.start_consuming()
    except pika.exceptions.AMQPConnectionError as e:
        logger.error("Failed to connect to RabbitMQ: %s", e)


@app.route("/")
def hello_world():
    """Serve a simple static Hello World page at the root"""
    return "<h1>wbor-postgres-driver is online!</h1>"


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()

    app.run(host="0.0.0.0", port=APP_PORT)
