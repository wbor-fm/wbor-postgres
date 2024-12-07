"""
Logic for retrying messages.
"""

import pika
from config import POSTGRES_QUEUE, MAX_RETRIES
from utils.logging import configure_logging

logger = configure_logging(__name__)


def retry_message(ch, method, body, retry_count):
    """Retry or send message to DLQ if retry limit is reached."""
    if retry_count < MAX_RETRIES:
        logger.warning("Retrying message. Retry count: %d", retry_count + 1)
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
