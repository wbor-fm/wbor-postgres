"""
Handle Gunicorn worker post-fork initialization.
"""

import threading
from consumers import PrimaryQueueConsumer, DeadLetterQueueConsumer
from config import POSTGRES_QUEUE


def post_fork(_server, _worker):
    """
    Define logic to kick off consumer threads in worker process.
    """

    # Initialize consumers
    
    # Bind wildcard routing key to the primary queue
    # Handle subrouting keys in the message handler
    primary_consumer = PrimaryQueueConsumer(
        queue_name=POSTGRES_QUEUE, routing_key="source.#"
    )
    dead_letter_consumer = DeadLetterQueueConsumer()

    # Define consumer threads
    def start_primary_consumer():
        primary_consumer.connect()
        primary_consumer.setup_queues()
        primary_consumer.consume_messages()

    def start_dead_letter_consumer():
        dead_letter_consumer.connect()
        dead_letter_consumer.setup_queues()
        dead_letter_consumer.retry_messages()

    # Start consumers in separate threads
    primary_thread = threading.Thread(target=start_primary_consumer, daemon=True)
    dlq_thread = threading.Thread(target=start_dead_letter_consumer, daemon=True)
    primary_thread.start()
    dlq_thread.start()
