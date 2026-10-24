from datetime import timedelta

from core.databricks_conn import (
    get_databricks_connection
)
from core.load_sql import load_sql

from airflow.sdk import dag, task
from loguru import logger


default_args = {
    "owner": "carlito",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}


@dag(
    dag_id="gold_public_schools_insertions",
    description="Build the fact table for the development of analytics dashboard",
    default_args=default_args,
    tags=["databricks", "gold", "analytics"],
)
def gold_schools_insertions():
    
    @task
    def create_table():

        create_sql = load_sql(
            "gold/create_gld_public_schools.sql"
        )
        
        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_sql)
            logger.success("Table deped.gold.gld_number_of_public_schools is ready")
        
        except Exception:
            logger.exception("Failed to create deped.gold.gld_number_of_public_schools")
            raise
    

    @task
    def transform_slvr_table():

        fact_transformation = load_sql(
            "gold/insert_gld_public_schools.sql"
        )

        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(fact_transformation)
            logger.success("Table deped.gold.gld_number_of_public_schools is ready")
        
        except Exception:
            logger.exception("Failed to create deped.gold.gld_number_of_public_schools")
            raise
        
    
    create_table = create_table()
    transform_slvr_table = transform_slvr_table()

    create_table >> transform_slvr_table


gold_schools_insertions()
