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
    dag_id="silver_public_teachers_insertions",
    description="Insertion of data of number of public teachers into the silver schema",
    default_args=default_args,
    tags=["databricks", "silver", "cleaning"],
)
def silver_public_schools_insertions():

    
    @task
    def create_table():

        create_sql = load_sql(
            "silver/create_slvr_public_teachers.sql"
        )
        
        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_sql)
            logger.success("Table deped.bronze.brnz_number_of_public_teachers is ready")
        
        except Exception:
            logger.exception("Failed to create deped.bronze.brnz_number_of_public_teachers")
            raise
    

    @task
    def cleaned_brnz_table():

        cleaned_sql = load_sql(
            "silver/insert_slvr_public_teachers.sql"
        )

        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(cleaned_sql)
            logger.success("Table deped.bronze.brnz_number_of_public_teachers is ready")
        
        except Exception:
            logger.exception("Failed to create deped.bronze.brnz_number_of_public_teachers")
            raise
        
    
    create_table = create_table()
    cleaned_brnz_table = cleaned_brnz_table()

    create_table >> cleaned_brnz_table


silver_public_schools_insertions()