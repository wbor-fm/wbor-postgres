"""
Utility functions for database interactions (connection setup, reusable queries).

Connection management (get_db_connection).
Low-level database operations (execute_query).
"""

import psycopg
from config import POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from utils.logging import configure_logging

logger = configure_logging(__name__)


def get_db_connection():
    """
    Establish a connection to the Postgres database.
    """
    return psycopg.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def execute_query(cursor, query, values):
    """
    Execute a SQL query with the given values.
    """
    try:
        cursor.execute(query, values)
    except psycopg.errors.DatabaseError as e:
        logger.error("Database error: %s", e)
        raise
