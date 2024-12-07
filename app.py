"""
Postgres Handler.

Insertion into Postgres database for various tables.

Tables:
- `messages`
    - Twilio SMS messages.
- `contacts`
    - Every # that has sent a message or called the Twilio number.
    - Includes the `SenderName` if able to be determined via the Twilio API.
        - This is provided in SMS messages, though is `Unknown` if not found.
- `banned`
    - Numbers that have been banned from sending messages.
    - (e.g. showing up on the dashboard/other consumers)
"""

import logging
from flask import Flask
from utils.logging import configure_logging
from config import APP_PORT


logging.root.handlers = []
logger = configure_logging()

app = Flask(__name__)


@app.route("/")
def hello_world():
    """Serve a simple static Hello World page at the root"""
    return "<h1>wbor-postgres-driver is online!</h1>"


if __name__ == "__main__":
    # Only start the Flask app; Gunicorn handles consumer initialization
    app.run(host="0.0.0.0", port=APP_PORT)
