import os

from loguru import logger
from databricks import sql
from sqlalchemy import create_engine


def get_databricks_connection():
    hostname = os.getenv("DATABRICKS_HOSTNAME")
    http_path = os.getenv("DATABRICKS_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    if not hostname:
        raise ValueError("DATABRICKS_HOSTNAME is not configured")

    if not http_path:
        raise ValueError("DATABRICKS_PATH is not configured")

    if not token:
        raise ValueError("DATABRICKS_TOKEN is not configured")

    logger.info("Opening connection to Databricks")

    return sql.connect(
        server_hostname=hostname,
        http_path=http_path,
        access_token=token,
    )


def get_databricks_engine(
    catalog: str,
    schema: str
):
    hostname = os.getenv("DATABRICKS_HOSTNAME")
    http_path = os.getenv("DATABRICKS_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    if not hostname:
        raise ValueError("DATABRICKS_HOSTNAME is not configured")

    if not http_path:
        raise ValueError("DATABRICKS_PATH is not configured")

    if not token:
        raise ValueError("DATABRICKS_TOKEN is not configured")

    if not catalog:
        raise ValueError("Please provide the catalog from databricks")
    if not schema:
        raise ValueError("Please provide the schema from catalog.")

    logger.info("Opening connection to Databricks")

    return create_engine(
        f"databricks://token:{token}@{hostname}"
        f"?http_path={http_path}"
        f"&catalog={catalog}"
        f"&schema={schema}"
    )

