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
    dag_id="seed_region_group",
    description="Create and populate the regional dimension table",
    default_args=default_args,
    tags=["databricks", "dimension", "seed"],
)
def seed_region_group():


    @task
    def create_dim_region():

        sql = """
            CREATE TABLE IF NOT EXISTS deped.dimensions.dim_region (
                region_key BIGINT,
                region_code STRING,
                region_name STRING,
                island_group STRING
            )
            USING DELTA
        """

        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
            logger.success("Table deped.dimensions.dim_region is ready")
        
        except Exception:
            logger.exception("Failed to create deped.dimensions.dim_region")
            raise
    
    @task
    def insert_region_data():
        logger.info("Starting the dim_region seed process")
    
        merge_sql = """
            MERGE INTO deped.dimensions.dim_region AS target
            USING (
                SELECT *
                FROM VALUES
                (1, 'I', 'Ilocos Region', 'Luzon'),
                (2, 'II', 'Cagayan Valley', 'Luzon'),
                (3, 'III', 'Central Luzon', 'Luzon'),
                (4, 'IV-A', 'CALABARZON', 'Luzon'),
                (5, 'IV-B', 'MIMAROPA', 'Luzon'),
                (6, 'V', 'Bicol Region', 'Luzon'),
                (7, 'VI', 'Western Visayas', 'Visayas'),
                (8, 'VII', 'Central Visayas', 'Visayas'),
                (9, 'VIII', 'Eastern Visayas', 'Visayas'),
                (10, 'IX', 'Zamboanga Peninsula', 'Mindanao'),
                (11, 'X', 'Northern Mindanao', 'Mindanao'),
                (12, 'XI', 'Davao Region', 'Mindanao'),
                (13, 'XII', 'Soccsksargen', 'Mindanao'),
                (14, 'CARAGA', 'CARAGA', 'Mindanao'),
                (15, 'BARMM', 'Bangsamoro Autonomous Region in Muslim Mindanao', 'Mindanao'),
                (16, 'CAR', 'Cordillera Administrative Region', 'Luzon'),
                (17, 'NCR', 'National Capital Region', 'Luzon')
                AS source (
                    region_key,
                    region_code,
                    region_name,
                    island_group
                )
            )
            ON target.region_code = source.region_code
             
            WHEN MATCHED THEN
                UPDATE SET
                target.region_key = source.region_key,
                target.region_name = source.region_name,
                target.island_group = source.island_group
             
            WHEN NOT MATCHED THEN
                INSERT (
                    region_key,
                    region_code,
                    region_name,
                    island_group
                )
                VALUES (
                    source.region_key,
                    source.region_code,
                    source.region_name,
                    source.island_group
                )
        """
        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(merge_sql)
            logger.success("Succesfully load the region data into dim_region")

        except Exception:
            logger.exception(
                "Failed to load region data into dim_region"
            )
            raise
        


    create_table = create_dim_region()
    load_data = insert_region_data()

    create_table >> load_data


seed_region_group()



        


