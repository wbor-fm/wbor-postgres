"""
RabbitMQ consumers for the primary and dead-letter queues.
"""

import json
import os
import signal
import pika
from psycopg.errors import DatabaseError
from pika.exceptions import AMQPError, AMQPConnectionError, ChannelClosedByBroker
from utils.logging import configure_logging
from retry import retry_message
from database import get_db_connection
from handlers import MESSAGE_HANDLERS
from config import (
    RABBITMQ_USER,
    RABBITMQ_PASS,
    RABBITMQ_HOST,
    RABBITMQ_EXCHANGE,
    RABBITMQ_DL_EXCHANGE,
    RABBITMQ_DL_QUEUE,
    POSTGRES_QUEUE,
    MAX_RETRIES,
)

logger = configure_logging(__name__)


def terminate_process():
    """Terminate the process and propagate termination."""
    os.kill(os.getpid(), signal.SIGTERM)


def handle_errors(callback_function):
    """
    Decorator to handle errors in the callback function.

    Wrapper handles incrementing the retry count.
    """

    def wrapper(ch, method, properties, body):
        # Safeguard against NoneType for headers by defaulting to 0 (new message)
        retry_count = properties.headers.get("x-retry-count", 0) if properties else 0
        try:
            # Execute process_message
            callback_function(ch, method, properties, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except (
            DatabaseError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            logger.error("Error processing message: %s", error)
            if retry_count < MAX_RETRIES:  # Local retries
                retry_message(ch, method, body, retry_count)
            else:
                logger.error("Max retries exceeded. Discarding: %s", body)
                ch.basic_ack(delivery_tag=method.delivery_tag)

    return wrapper


@handle_errors
def process_message(_ch, method, _properties, body):
    """
    Core message processing logic.
    """
    message = json.loads(body)
    routing_key = method.routing_key.removeprefix("source.")
    logger.info(
        "PrimaryQueueConsumer - Processing message (w/ key `%s`): %s - %s",
        routing_key,
        message.get("wbor_message_id"),
        message,
    )

    # Depending on the routing key, perform different actions
    handler = MESSAGE_HANDLERS.get(routing_key)
    if not handler:
        logger.info("No handler found for routing key: `%s`", routing_key)
        # Don't requeue - ack done in decorator
        return

    with get_db_connection() as conn, conn.cursor() as cursor:
        handler(message, cursor)
        conn.commit()
        logger.info(
            "PrimaryQueueConsumer - Successfully processed message for routing key `%s`: %s",
            routing_key,
            message.get("wbor_message_id"),
        )


def callback(ch, method, properties, body):
    """
    Handle incoming RabbitMQ messages by routing to the appropriate handler based
    on the routing_key.
    """
    logger.debug(
        "Callback triggered with routing key: `%s`",
        method.routing_key,
    )  # TODO: how to make sure this isn't bound to receive messages from wbor-groupme's internal send queue
    process_message(ch, method, properties, body)


class RabbitMQBaseConsumer:
    """
    Base class for RabbitMQ consumers to handle connection and setup.

    Features:
    - Connect to RabbitMQ
    - Assert exchanges and declare/bind queues
    """

    def __init__(self, queue_name, routing_key, exchange=RABBITMQ_EXCHANGE):
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.exchange = exchange
        self.connection = None
        self.channel = None

    def connect(self):
        """Establish connection and channel to RabbitMQ."""
        logger.debug("Connecting `%s` to RabbitMQ...", self.queue_name)
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                client_properties={"connection_name": f"{self.queue_name}_Consumer"},
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
        except AMQPConnectionError as conn_error:
            error_message = str(conn_error)
            logger.error(
                "`%s` AMQP Connection Error: %s", self.queue_name, error_message
            )
            if "ACCESS_REFUSED" in error_message:
                logger.critical("Access refused. Please check RabbitMQ credentials.")
            terminate_process()

    def assert_exchange(self):
        """
        Assert the exchange exists before attempting to bind.

        This is called by both consumers, so there is double-validation.
        """
        if not self.channel:
            raise RuntimeError("Channel not initialized. Cannot assert exchange.")

        self.channel.exchange_declare(
            exchange=RABBITMQ_EXCHANGE, exchange_type="topic", durable=True
        )
        self.channel.exchange_declare(
            exchange=RABBITMQ_DL_EXCHANGE, exchange_type="direct", durable=True
        )

    def setup_queues(self):
        """
        (Assert exchange and) declare queues/bindings.

        This is called by both consumers, so there is double-validation.
        """
        self.assert_exchange()
        try:
            # DLX
            self.channel.queue_declare(
                queue=RABBITMQ_DL_QUEUE,
                durable=True,
                arguments={
                    "x-message-ttl": 60000,
                    "x-dead-letter-exchange": RABBITMQ_DL_EXCHANGE,
                },
            )
            self.channel.queue_bind(
                exchange=RABBITMQ_DL_EXCHANGE, queue=RABBITMQ_DL_QUEUE
            )

            # Primary
            self.channel.queue_declare(
                queue=POSTGRES_QUEUE,
                durable=True,
                arguments={
                    "x-message-ttl": 60000,
                    "x-dead-letter-exchange": RABBITMQ_DL_EXCHANGE,
                },
            )
            self.channel.queue_bind(
                # Bind this queue to the exchange with the routing key, meaning
                # that messages with this routing key will be sent to this queue
                exchange=RABBITMQ_EXCHANGE,
                queue=POSTGRES_QUEUE,
                routing_key="source.#",
            )
        except ChannelClosedByBroker as e:
            if "inequivalent arg" in str(e):
                # If a queue already exists with different attributes, log and terminate
                logger.critical(
                    "Queue already exists with mismatched attributes. "
                    "Please resolve this conflict before restarting the application."
                )
                # Close connection
                if self.connection and not self.connection.is_closed:
                    self.connection.close()
                terminate_process()

    def stop(self):
        """Attempt to gracefully stop consuming and close connection."""
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logger.info("`%s` connection closed.", self.queue_name)


class PrimaryQueueConsumer(RabbitMQBaseConsumer):
    """
    Consumer for the primary queue.

    Called with queue_name="postgres", routing_key="source.#"

    Means that messages with routing keys starting with "source." will be sent to this queue.
    (all messages in this case, e.g. "source.groupme", "source.twilio")
    """

    def consume_messages(self):
        """Consume messages from the main queue."""

        # Define how to consume messages
        self.channel.basic_consume(
            queue=POSTGRES_QUEUE, on_message_callback=callback, auto_ack=False
        )
        # callback -> process_message (wrapped by decorator) -> handler -> database
        logger.info("Primary queue consumer ready to consume messages.")
        try:
            self.channel.start_consuming()
            logger.debug("Consuming messages...")
        except (AMQPError, DatabaseError) as e:
            logger.critical(
                "(PrimaryQueueConsumer terminating) Error consuming messages: %s", e
            )
            terminate_process()


# Dead-letter queue consumer class
class DeadLetterQueueConsumer(RabbitMQBaseConsumer):
    """Consumer for the dead-letter queue."""

    def __init__(self):
        super().__init__(
            queue_name=RABBITMQ_DL_QUEUE, routing_key="", exchange=RABBITMQ_DL_EXCHANGE
        )

    def handle_dlq_message(self):
        """Handle messages from the dead-letter queue."""

        def dlq_callback(ch, method, properties, body):
            try:
                retry_count = (
                    properties.headers.get("x-retry-count", MAX_RETRIES)
                    if properties
                    else MAX_RETRIES
                )
                logger.error(
                    "Processing message from dead-letter queue with retry count: %d. Body: %s",
                    retry_count,
                    body,
                )

                # Archive or log the message for further analysis
                self.archive_message(body, properties)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except (AMQPError, DatabaseError, ValueError, json.JSONDecodeError) as e:
                logger.error("Error handling DLQ message: %s", e)
                ch.basic_nack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(
            queue=RABBITMQ_DL_QUEUE, on_message_callback=dlq_callback, auto_ack=False
        )
        logger.info("Dead-letter queue consumer ready to process messages.")
        try:
            self.channel.start_consuming()
        except (AMQPError, DatabaseError) as e:
            logger.critical("Error processing messages from DLQ: %s", e)
            terminate_process()

    def archive_message(self, body, properties):
        """
        Archive the DLQ message for further inspection.

        Args:
            body: The message body.
            properties: Message properties (headers, etc.).
        """
        logger.info("Archiving message: %s", body)
        # Example: Save to a log file or database
        with open("dlq_messages.log", "a", encoding="utf-8") as f:
            f.write(f"Message: {body}, Properties: {properties}\n")
