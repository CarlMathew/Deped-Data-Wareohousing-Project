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
    dag_id="seed_school_year",
    description="Create and populate the school year dimension table",
    default_args=default_args,
    tags=["databricks", "dimension", "seed"],
)
def seed_region_group():
    
    @task
    def create_dim_school_year():
        sql = """
            CREATE TABLE IF NOT EXISTS deped.dimensions.dim_school_year (
                school_year_key BIGINT,
                school_year STRING,
                start_year INT,
                end_year INT,
                is_covid_period BOOLEAN
            )
            USING DELTA;
        """

        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
            logger.success("Table deped.dimensions.dim_school_year is ready")
        
        except Exception:
            logger.exception("Failed to create deped.dimensions.dim_school_year")
            raise
    

    @task
    def insert_school_year_data():
        logger.info("Starting the dim_school_year seed process")
    
        merge_sql = """
            MERGE INTO deped.dimensions.dim_school_year AS target

            USING (
                SELECT *
                FROM VALUES
                    (1,  '2010-2011', 2010, 2011, FALSE),
                    (2,  '2011-2012', 2011, 2012, FALSE),
                    (3,  '2012-2013', 2012, 2013, FALSE),
                    (4,  '2013-2014', 2013, 2014, FALSE),
                    (5,  '2014-2015', 2014, 2015, FALSE),
                    (6,  '2015-2016', 2015, 2016, FALSE),
                    (7,  '2016-2017', 2016, 2017, FALSE),
                    (8,  '2017-2018', 2017, 2018, FALSE),
                    (9,  '2018-2019', 2018, 2019, FALSE),
                    (10, '2019-2020', 2019, 2020, FALSE),
                    (11, '2020-2021', 2020, 2021, TRUE)

                AS source (
                    school_year_key,
                    school_year,
                    start_year,
                    end_year,
                    is_covid_period
                )
            )

            ON target.school_year = source.school_year

            WHEN MATCHED THEN
                UPDATE SET
                    target.school_year_key = source.school_year_key,
                    target.start_year = source.start_year,
                    target.end_year = source.end_year,
                    target.is_covid_period = source.is_covid_period

            WHEN NOT MATCHED THEN
                INSERT (
                    school_year_key,
                    school_year,
                    start_year,
                    end_year,
                    is_covid_period
                )
                VALUES (
                    source.school_year_key,
                    source.school_year,
                    source.start_year,
                    source.end_year,
                    source.is_covid_period
                );
        """
        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(merge_sql)
            logger.success("Succesfully load the school year data into dim_region")

        except Exception:
            logger.exception(
                "Failed to load school year data into dim_region"
            )
            raise
    
    create_table = create_dim_school_year()
    load_data = insert_school_year_data()

    create_table >> load_data

seed_region_group()