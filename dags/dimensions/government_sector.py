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
    dag_id="seed_government_sector_group",
    description="Create and populate the government sector dimension table",
    default_args=default_args,
    tags=["databricks", "dimension", "seed"],
)
def seed_government_sector_group():


    @task
    def create_government_sector_level():

        sql = """
            CREATE TABLE IF NOT EXISTS deped.dimensions.dim_government_sector (
                government_term_key BIGINT,
                year INT,
                president STRING,
                vice_president STRING,
                secretary_of_education STRING
            )
            USING DELTA;
        """

        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
            logger.success("Table deped.dimensions.dim_government_sector is ready")
        
        except Exception:
            logger.exception("Failed to create deped.dim_government_sector.dim_region")
            raise
    
    @task
    def insert_govenrment_sector_data():
        logger.info("Starting the dim_government_sector seed process")
    
        merge_sql = """
            MERGE INTO deped.dimensions.dim_government_sector AS target

            USING (
                SELECT *
                FROM VALUES
                    (1,  2010, 'Benigno Aquino III', 'Jejomar Binay', 'Armin Luistro'),
                    (2,  2011, 'Benigno Aquino III', 'Jejomar Binay', 'Armin Luistro'),
                    (3,  2012, 'Benigno Aquino III', 'Jejomar Binay', 'Armin Luistro'),
                    (4,  2013, 'Benigno Aquino III', 'Jejomar Binay', 'Armin Luistro'),
                    (5,  2014, 'Benigno Aquino III', 'Jejomar Binay', 'Armin Luistro'),
                    (6,  2015, 'Benigno Aquino III', 'Jejomar Binay', 'Armin Luistro'),
                    (7,  2016, 'Rodrigo Duterte',    'Leni Robredo',  'Leonor Briones'),
                    (8,  2017, 'Rodrigo Duterte',    'Leni Robredo',  'Leonor Briones'),
                    (9,  2018, 'Rodrigo Duterte',    'Leni Robredo',  'Leonor Briones'),
                    (10, 2019, 'Rodrigo Duterte',    'Leni Robredo',  'Leonor Briones'),
                    (11, 2020, 'Rodrigo Duterte',    'Leni Robredo',  'Leonor Briones')

                AS source (
                    government_term_key,
                    year,
                    president,
                    vice_president,
                    secretary_of_education
                )
            )

            ON target.year = source.year

            WHEN MATCHED THEN
                UPDATE SET
                    target.government_term_key = source.government_term_key,
                    target.president = source.president,
                    target.vice_president = source.vice_president,
                    target.secretary_of_education =
                        source.secretary_of_education

            WHEN NOT MATCHED THEN
                INSERT (
                    government_term_key,
                    year,
                    president,
                    vice_president,
                    secretary_of_education
                )
                VALUES (
                    source.government_term_key,
                    source.year,
                    source.president,
                    source.vice_president,
                    source.secretary_of_education
                );
        """
        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(merge_sql)
            logger.success("Succesfully load the government sector data into dim_region")

        except Exception:
            logger.exception(
                "Failed to load region data into dim_government_sector"
            )
            raise
        
    create_table = create_government_sector_level()
    load_data = insert_govenrment_sector_data()

    create_table >> load_data


seed_government_sector_group()