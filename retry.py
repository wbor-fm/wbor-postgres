"""
Logic for retrying messages.
"""

import pika
from config import (
    RABBITMQ_EXCHANGE,
    RABBITMQ_DL_EXCHANGE,
    MAX_RETRIES,
    RABBITMQ_DL_QUEUE,
    POSTGRES_QUEUE,
)
from utils.logging import configure_logging

logger = configure_logging(__name__)


def retry_message(ch, method, body, retry_count):
    """
    Retry a message or route it to the Dead Letter Queue (DLQ) if retries are exhausted.

    Args:
    - ch (pika.channel.Channel): Channel object.
    - method (pika.spec.Basic.Deliver): Method object.
    - body (str): Message body.
    - retry_count (int): Number of times the message has been retried.
    """
    max_retries_reached = retry_count >= MAX_RETRIES

    # Determine routing and logging
    if max_retries_reached:
        logger.warning(
            "Max retries reached. Routing to DLQ for routing key: %s",
            method.routing_key,
        )
        exchange = RABBITMQ_DL_EXCHANGE
        routing_key = RABBITMQ_DL_QUEUE
    else:
        logger.warning(
            "Retrying message. Retry count: %d for routing key: %s",
            retry_count + 1,
            method.routing_key,
        )
        exchange = RABBITMQ_EXCHANGE
        routing_key = POSTGRES_QUEUE

    # Publish the message with updated headers
    headers = {"x-retry-count": retry_count + 1}
    ch.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(headers=headers),
    )
    # Always acknowledge the original message
    ch.basic_ack(delivery_tag=method.delivery_tag)
