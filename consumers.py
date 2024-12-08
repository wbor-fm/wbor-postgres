"""
RabbitMQ consumers for the primary and dead-letter queues.
"""

import json
import psycopg
import pika
import pika.exceptions
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
    POSTGRES_QUEUE,
)

logger = configure_logging(__name__)


def callback(ch, method, properties, body):
    """
    Handle incoming RabbitMQ messages by routing to the appropriate handler based
    on the routing_key.
    """
    logger.debug(
        "Callback triggered with routing key: `%s`",
        method.routing_key,
    )  # TODO: how to make sure this isn't bound to receive messages from wbor-groupme's internal send queue

    retry_count = 0  # Safeguard against NoneType for headers
    if properties and properties.headers:
        retry_count = properties.headers.get("x-retry-count", 0)

    conn = None
    try:
        message = json.loads(body)
        routing_key = method.routing_key.removeprefix("source.")
        logger.info("Processing message (w/ key `%s`): %s", routing_key, message)

        # Depending on the routing key, perform different actions
        # Get the handler based on routing key
        handler = MESSAGE_HANDLERS.get(routing_key)
        if not handler:
            logger.info("No handler found for routing key: `%s`", routing_key)

        if handler:
            # Use the handler to process the message
            conn = get_db_connection()  # Open connection
            with conn.cursor() as cursor:
                handler(message, cursor)
                conn.commit()
                logger.info(
                    "Successfully processed message for routing key: `%s`", routing_key
                )

        # Acknowledge the message
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except (psycopg.errors.DatabaseError, ValueError, json.JSONDecodeError) as error:
        logger.error("Error processing message: %s", error)
        retry_message(ch, method, body, retry_count)
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.debug("Database connection closed.")


class RabbitMQBaseConsumer:
    """
    Base class for RabbitMQ consumers to handle connection and setup.

    Features:
    - Connect to RabbitMQ
    - Assert exchanges and declare queues
    - Gracefully stop consuming messages
    """

    def __init__(self, queue_name, routing_key, exchange=RABBITMQ_EXCHANGE):
        self.connection = None
        self.channel = None
        self.queue_name = queue_name
        self.routing_key = routing_key
        self.exchange = exchange

    def connect(self):
        """Establish connection and channel to RabbitMQ."""
        logger.debug("Connecting to RabbitMQ...")

        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                credentials=credentials,
                client_properties={"connection_name": f"{self.queue_name}_Consumer"},
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
        except pika.exceptions.AMQPConnectionError as amqp_error:
            logger.error("AMQP Connection Error: %s", amqp_error)

    def assert_exchange(self):
        """Assert the exchange for the consumer."""
        self.channel.exchange_declare(
            exchange=self.exchange, exchange_type="topic", durable=True
        )
        self.channel.exchange_declare(
            exchange=RABBITMQ_DL_EXCHANGE, exchange_type="direct", durable=True
        )

    def setup_queues(self):
        """(Assert exchange and) declare queues."""
        self.assert_exchange()

        # Declare dead-letter queue and bind to exchange
        try:
            self.channel.queue_declare(
                queue="dead_letter_queue",
                durable=True,
                arguments={
                    "x-message-ttl": 60000,
                    "x-dead-letter-exchange": RABBITMQ_DL_EXCHANGE,
                },
            )
        except pika.exceptions.ChannelClosedByBroker as e:
            if "inequivalent arg" in str(e):
                # If the queue already exists with different attributes, log and terminate
                logger.warning(
                    "Queue '%s' already exists with different attributes."
                    "Skipping redeclaration.",
                    RABBITMQ_DL_EXCHANGE,
                )
                # Close connection
                if self.connection and not self.connection.is_closed:
                    self.connection.close()
                raise RuntimeError(
                    f"Queue '{RABBITMQ_DL_EXCHANGE}' already exists with mismatched attributes. "
                    "Please resolve this conflict before restarting the application."
                ) from e
            raise
        self.channel.queue_bind(
            exchange=RABBITMQ_DL_EXCHANGE, queue="dead_letter_queue"
        )

        # Declare primary queue and bind to exchange (e.g. "postgres")
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={
                "x-message-ttl": 60000,
                "x-dead-letter-exchange": RABBITMQ_DL_EXCHANGE,
            },
        )
        self.channel.queue_bind(
            # Bind this queue to the exchange with the routing key, meaning
            # that messages with this routing key will be sent to this queue
            exchange=self.exchange,
            queue=self.queue_name,
            routing_key=self.routing_key,
        )

    def stop(self):
        """Gracefully stop consuming and close connections."""
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logger.info("Connections closed.")


class PrimaryQueueConsumer(RabbitMQBaseConsumer):
    """
    Consumer for the primary queue.

    Called with queue_name="postgres", routing_key="source.#"

    Means that messages with routing keys starting with "source." will be sent to this queue.
    (all messages in this case, e.g. "source.groupme", "source.twilio")
    """

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
            # exchange="" signifies that the message is being published directly to a
            # queue rather than being routed through an exchange

            # means that if you specify exchange="" and provide a routing_key equal to the
            # name of a queue, the message is directly delivered to that queue.
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
