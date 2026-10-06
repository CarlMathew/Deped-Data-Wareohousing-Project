from datetime import datetime, timedelta
import os

from airflow.sdk import dag, task
from databricks import sql
from loguru import logger


default_args = {
    "owner": "carlito",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}


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


@dag(
    dag_id="seed_education_level_group",
    description="Create and populate the education level dimension table",
    default_args=default_args,
    tags=["databricks", "dimension", "seed"],
)
def seed_education_level_group():


    @task
    def create_dim_education_level():

        sql = """
            CREATE TABLE IF NOT EXISTS deped.dimensions.dim_education_level (
                education_level_key BIGINT,
                education_level_code STRING,
                education_level_name STRING
            )
            USING DELTA;
        """

        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
            logger.success("Table deped.dimensions.dim_education_level is ready")
        
        except Exception:
            logger.exception("Failed to create deped.dim_education_level.dim_region")
            raise
    
    @task
    def insert_education_level_data():
        logger.info("Starting the dim_education_level seed process")
    
        merge_sql = """
            MERGE INTO deped.dimensions.dim_education_level AS target

            USING (
                SELECT *
                FROM VALUES
                    (1, 'ELEM', 'Elementary'),
                    (2, 'JHS',  'Junior High School'),
                    (3, 'SHS',  'Senior High School')

                AS source (
                    education_level_key,
                    education_level_code,
                    education_level_name
                )
            )

            ON target.education_level_code = source.education_level_code

            WHEN MATCHED THEN
                UPDATE SET
                    target.education_level_key = source.education_level_key,
                    target.education_level_name = source.education_level_name

            WHEN NOT MATCHED THEN
                INSERT (
                    education_level_key,
                    education_level_code,
                    education_level_name
                )
                VALUES (
                    source.education_level_key,
                    source.education_level_code,
                    source.education_level_name
                );
        """
        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(merge_sql)
            logger.success("Succesfully load the education level data into dim_region")

        except Exception:
            logger.exception(
                "Failed to load region data into dim_education_level"
            )
            raise
        
    create_table = create_dim_education_level()
    load_data = insert_education_level_data()

    create_table >> load_data


seed_education_level_group()