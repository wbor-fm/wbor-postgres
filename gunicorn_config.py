"""
App launch configuration for Gunicorn.
"""

import threading
import os
import signal
from consumers import PrimaryQueueConsumer, DeadLetterQueueConsumer
from config import POSTGRES_QUEUE


def post_fork(_server, _worker):
    """
    Define logic to kick off consumer threads in worker process.
    """
    # TODO: use different PIDs for each queue consumer
    # TODO: I think there are also duplicate PrimaryQueueConsumer instances

    # Initialize consumers

    # Bind wildcard routing key to the primary queue
    # Handle subrouting keys in the message handler
    primary_consumer = PrimaryQueueConsumer(
        queue_name=POSTGRES_QUEUE, routing_key="source.#"
    )
    dead_letter_consumer = DeadLetterQueueConsumer()

    def terminate(exit_code=1):
        """Terminate Gunicorn."""
        print("Terminating process due to critical error.")
        os.kill(os.getppid(), signal.SIGTERM)
        os._exit(exit_code)

    # Define consumer threads
    def start_primary_consumer():
        try:
            primary_consumer.connect()
            primary_consumer.setup_queues()
            primary_consumer.consume_messages()
        except (ConnectionError, RuntimeError) as e:
            print(f"Critical error in PrimaryQueueConsumer: {e}")
            terminate()

    def start_dead_letter_consumer():
        try:
            dead_letter_consumer.connect()
            dead_letter_consumer.setup_queues()
            dead_letter_consumer.handle_dlq_message()
        except (ConnectionError, RuntimeError) as e:
            print(f"Critical error in DeadLetterQueueConsumer: {e}")
            terminate()

    # Start consumers in separate threads
    primary_thread = threading.Thread(target=start_primary_consumer, daemon=True)
    dlq_thread = threading.Thread(target=start_dead_letter_consumer, daemon=True)
    primary_thread.start()
    dlq_thread.start()
