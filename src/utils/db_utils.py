"""
Module Name: db_utils.py

Description:
    Creates Oracle Autonomous Database connection
    and retrieves configuration values and reference data from the database.
"""


import logging
import os

from dotenv import load_dotenv
import oracledb

# -------------------------------------------------------------------
# Environment Variables
# -------------------------------------------------------------------

load_dotenv()

DB_USER = os.getenv("OCI_DB_USER")
DB_PASSWORD = os.getenv("OCI_DB_PASSWORD")
DB_DSN = os.getenv("OCI_DB_DSN")

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Database Connection
# -------------------------------------------------------------------

def get_database_connection() -> oracledb.Connection:
    """
    Create and return an Oracle Autonomous Database connection.
    Raises ValueError if any required environment variable is missing,
    """
    if not all([DB_USER, DB_PASSWORD, DB_DSN]):
        raise ValueError(
            "Missing required Oracle database environment variables: "
            "OCI_DB_USER, OCI_DB_PASSWORD, OCI_DB_DSN"
        )

    logger.info("Connecting to Oracle Autonomous Database...")

    return oracledb.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        dsn=DB_DSN
    )


# -------------------------------------------------------------------
# Configuration Retrieval
# -------------------------------------------------------------------


def fetch_config_value(conn: oracledb.Connection, config_key: str) -> str:
    """
    Retrieve a configuration value from the database.
    Args:
        conn (oracledb.Connection): The database connection.
        config_key (str): The key of the configuration value to retrieve.
    Returns:
        str: The configuration value.
    """
    logger.info("Fetching configuration value for key: %s", config_key)
    sql = f"""
        SELECT CONFIG_VALUE FROM CONFIGURATION
        WHERE CONFIG_KEY = '{config_key}'
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchone()
            if result:
                config_value = result[0]
                logger.info("Configuration value for key %s: %s", config_key, config_value)
                return config_value
            else:
                logger.warning("No configuration value found for key: %s", config_key)
                return None
    except Exception as e:
        logger.exception("Failed to retrieve configuration value: %s", e)
        raise


# -------------------------------------------------------------------
# Reference Data Retrieval
# -------------------------------------------------------------------


def fetch_reference_values(conn: oracledb.Connection, table_name: str, column_name: str) -> set[str]:
    """
    Retrieve valid reference values from dimension table
    Example:
        fetch_reference_values("dim_regions", "region_name")
        fetch_reference_values("dim_categories", "category_name")
    """
    logger.info("Fetching valid %s from %s...", column_name, table_name)
    cursor = None

    try:
        with conn.cursor() as cursor:
            query = (f"SELECT {column_name} FROM {table_name}")
            cursor.execute(query)
            rows = cursor.fetchall()
            valid_values = {row[0] for row in rows}
    except Exception as e:
        logger.exception("Failed to retrieve reference values: %s", e)
        raise

    logger.info("Found %s valid values: %s", len(valid_values), sorted(valid_values))
    return valid_values


def fetch_reference_mapping(conn: oracledb.Connection, table_name: str, key_column: str, value_column: str) -> dict:
    """
    Return a mapping
    """
    logger.info("Fetching %s to %s mapping from %s...", key_column, value_column, table_name)
    cursor = None
    
    try:
        with conn.cursor() as cursor:
            query = (f"SELECT {key_column}, {value_column} FROM {table_name}")
            cursor.execute(query)
            valid_mapping = {
                row[0]: row[1]
                for row in cursor.fetchall()
            }
    except Exception as e:
        logger.exception("Failed to retrieve reference mapping: %s", e)
        raise
    
    logger.info("Found %s valid mappings: %s", len(valid_mapping), sorted(valid_mapping))
    return valid_mapping


