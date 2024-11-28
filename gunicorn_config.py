def post_fork(server, worker):
    """
    Define logic to kick off consumer threads in worker process.
    """
    from app import PrimaryQueueConsumer, DeadLetterQueueConsumer
    import threading
    import logging

    logger = logging.getLogger(__name__)
    logger.info("Starting consumers in worker process.")

    # Initialize consumers
    primary_consumer = PrimaryQueueConsumer(queue_name="postgres", routing_key="source.twilio.#")
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

    logger.info("Consumer threads started.")
