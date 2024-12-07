"""
Register signal handlers for SIGINT and SIGTERM signals.
"""

import signal
import sys
from utils.logging import configure_logging

logger = configure_logging(__name__)


def signal_handler(consumers, sig, _):
    """
    Gracefully stop consumers on SIGINT or SIGTERM.

    Parameters:
    - consumers: List of consumer instances to stop.
    - sig: Signal number.
    - _: Frame object. Unused
    """
    logger.info("Signal received: %s. Shutting down consumers gracefully...", sig)
    for consumer in consumers:
        consumer.stop()
    sys.exit(0)


def register_signal_handlers(consumers):
    """
    Initialize signal handlers for SIGINT and SIGTERM.
    """
    signal.signal(
        signal.SIGINT, lambda sig, frame: signal_handler(consumers, sig, frame)
    )
    signal.signal(
        signal.SIGTERM, lambda sig, frame: signal_handler(consumers, sig, frame)
    )
