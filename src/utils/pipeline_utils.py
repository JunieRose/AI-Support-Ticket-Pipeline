"""
Module Name: pipeline_utils.py

Description:
    Utility functions for managing pipeline execution metadata.
"""

import logging
import json
from datetime import datetime
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
# Pipelines
# -------------------------------------------------------------------

def get_stage_id(conn: oracledb.Connection, pipeline_code: str, stage_name: str) -> str:
    """
    Retrieve the current stage ID from the database.
    Returns:
        str: The current stage ID.
    """
    logger.info("Retrieving current stage ID from the database...")
    sql = """
        SELECT STAGE_ID FROM PIPELINE_STAGES
        INNER JOIN PIPELINES
        ON PIPELINES.PIPELINE_ID = PIPELINE_STAGES.PIPELINE_ID
        WHERE PIPELINES.PIPELINE_CODE = :1
        AND PIPELINE_STAGES.STAGE_NAME = :2
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (pipeline_code, stage_name))
            result = cursor.fetchone()
            if result:
                stage_id = result[0]
                logger.info("Current stage ID: %i", stage_id)
                return stage_id
            else:
                logger.warning("No stage ID found in the database.")
                return None
    except Exception as e:
        logger.exception("Failed to retrieve stage ID: %s", e)
        raise


def start_pipeline_stage(conn: oracledb.Connection, start_time: datetime, execution_id: str, stage_id: str) -> int:
    """
    Mark the start of a pipeline stage in the database.
    Args:
        conn (oracledb.Connection): The database connection.
        start_time (datetime): The time when the pipeline stage starts.
        execution_id (str): The pipeline_timestamp of the current pipeline run.
        stage_id (str): The ID of the stage to mark as started.
    """
    status = "STARTED"
    sql = """
        INSERT INTO PIPELINE_RUNS (
        EXECUTION_ID,
        STAGE_ID,
        RUN_STATUS,
        RUN_START_TIME)
        VALUES (
        :1,
        :2,
        :3,
        :4
        )
        RETURNING RUN_ID INTO :5
        """
    try:
        with conn.cursor() as cursor:
            run_id_var = cursor.var(int)
            cursor.execute(sql, (execution_id, stage_id, status, start_time, run_id_var))
            conn.commit()
            run_id = run_id_var.getvalue()[0]
            logger.info("Run ID: %i marked as %s.", run_id, status)
            return run_id
    except Exception as e:
        logger.exception("Failed to start pipeline stage: %s", e)
        conn.rollback()
        raise



def complete_pipeline_stage(conn: oracledb.Connection, run_id: int, metrics: dict) -> None:
    """
    Mark the completion of a pipeline stage in the database.
    Args:
        conn (oracledb.Connection): The database connection.
        run_id (int): The ID of the pipeline run to mark as completed.
        metrics (dict): The metrics for the completed stage.
    """
    end_time = datetime.now()
    status = "SUCCESS"
    metrics = json.dumps(metrics)
    logger.info("Marking Run ID: %i as %s...", run_id, status)

    sql = """
        UPDATE PIPELINE_RUNS
        SET RUN_STATUS = :1,
            RUN_END_TIME = :2,
            METRICS = :3
        WHERE RUN_ID = :4
        """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (status, end_time, metrics, run_id))
            conn.commit()
            logger.info("Run ID: %i marked as %s.", run_id, status)
    except Exception as e:
        logger.exception("Failed to complete pipeline stage: %s", e)
        conn.rollback()
        raise


def fail_pipeline_stage(conn: oracledb.Connection, run_id: int, error_message: str) -> None:
    """
    Mark the failure of a pipeline stage in the database.
    Args:
        conn (oracledb.Connection): The database connection.
        run_id (int): The ID of the pipeline run to mark as failed.
        metrics (dict): The metrics for the failed stage.
        error_message (str): The error message for the failed stage.
    """
    end_time = datetime.now()
    status = "FAILED"
    logger.info("Marking Run ID: %i as %s...", run_id, status)
    sql = """
        UPDATE PIPELINE_RUNS
        SET RUN_STATUS = :1,
            RUN_END_TIME = :2,
            ERROR_MESSAGE = :3
        WHERE RUN_ID = :4
        """
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, (status, end_time, error_message, run_id))
            conn.commit()
            logger.info("Run ID: %i marked as %s.", run_id, status)
    except Exception as e:
        logger.exception("Failed to mark pipeline stage as failed: %s", e)
        conn.rollback()
        raise