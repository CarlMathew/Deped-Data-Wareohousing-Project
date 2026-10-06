from datetime import timedelta
import io

from core.databricks_conn import (
    get_databricks_connection, 
    get_databricks_engine
)
from core.minio_conn import get_minio_client
from core.load_sql import load_sql

from airflow.sdk import dag, task
from loguru import logger
import pandas as pd




default_args = {
    "owner": "carlito",
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

@dag(
    dag_id="bronze_public_schools_insertions",
    description="Insertion of data of number of public schools into the bronze schema",
    default_args=default_args,
    tags=["databricks", "bronze", "ingestion"],
)
def bronze_public_schools_insertions():

    @task
    def create_table():

        create_sql = load_sql(
            "bronze/create_brnz_public_schools.sql"
        )
        
        try:
            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(create_sql)
            logger.success("Table deped.bronze.brnz_number_of_public_schools is ready")
        
        except Exception:
            logger.exception("Failed to create deped.bronze.brnz_number_of_public_schools")
            raise
    

    
    @task 
    def elementary_data():
        file_name = "Historical_Number_Public_School.xlsx"
        tab_name = "Elementary" 
    
        client, bucket = get_minio_client()
        engine = get_databricks_engine(
            catalog="deped",
            schema="bronze"
        )
        
        response = client.get_object(
            Bucket=bucket,
            Key=file_name
        )


        insert_sql = load_sql(
            "bronze/insert_public_schools.sql"
        )
        drop_stg_sql = load_sql(
            "bronze/drop_stg_public_schools.sql"
        )



        df = pd.read_excel(
            io.BytesIO(response["Body"].read()),
            skiprows=6,
            skipfooter=1,
            sheet_name=tab_name,
            engine="openpyxl"
        )

        if len(df) == 0:
            logger.warning(f"{tab_name} can't read the data or doesn't have any data. Skipping the task.")
            return None
    
        df_long = df.melt(
            id_vars=["Region"],
            var_name="school_year",
            value_name="number_of_schools"
        )

        df_long["education_level"] = tab_name
        df_long["source_file"] = f"{bucket}/{file_name}"
        df_long = df_long.rename(columns={"Region": "region"})


        try:
            df_long.to_sql(
                name="stg_public_schools",
                con=engine,
                schema="bronze",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

            logger.success("Elementary data loaded successfully on the staging")


            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql)
                    logger.success("Staging data merged into the target table")

                    cursor.execute(drop_stg_sql)
                    logger.success("Staging table dropped")

        finally:
            engine.dispose() 
            response["Body"].close()

    @task 
    def junior_data():
        file_name = "Historical_Number_Public_School.xlsx"
        tab_name = "Junior High School" 
    
        client, bucket = get_minio_client()
        engine = get_databricks_engine(
            catalog="deped",
            schema="bronze"
        )
        
        response = client.get_object(
            Bucket=bucket,
            Key=file_name
        )


        insert_sql = load_sql(
            "bronze/insert_brnz_public_schools.sql"
        )
        drop_stg_sql = load_sql(
            "bronze/drop_stg_public_schools.sql"
        )



        df = pd.read_excel(
            io.BytesIO(response["Body"].read()),
            skiprows=6,
            skipfooter=1,
            sheet_name=tab_name,
            engine="openpyxl"
        )

        if len(df) == 0:
            logger.warning(f"{tab_name} can't read the data or doesn't have any data. Skipping the task.")
            return None
    
        df_long = df.melt(
            id_vars=["Region"],
            var_name="school_year",
            value_name="number_of_schools"
        )

        df_long["education_level"] = tab_name
        df_long["source_file"] = f"{bucket}/{file_name}"
        df_long = df_long.rename(columns={"Region": "region"})


        try:
            df_long.to_sql(
                name="stg_public_schools",
                con=engine,
                schema="bronze",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

            logger.success("Elementary data loaded successfully on the staging")


            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql)
                    logger.success("Staging data merged into the target table")

                    cursor.execute(drop_stg_sql)
                    logger.success("Staging table dropped")

        finally:
            engine.dispose() 
            response["Body"].close()

    @task 
    def senior_data():
        file_name = "Historical_Number_Public_School.xlsx"
        tab_name = "Senior High School" 
    
        client, bucket = get_minio_client()
        engine = get_databricks_engine(
            catalog="deped",
            schema="bronze"
        )
        
        response = client.get_object(
            Bucket=bucket,
            Key=file_name
        )


        insert_sql = load_sql(
            "bronze/insert_public_schools.sql"
        )
        drop_stg_sql = load_sql(
            "bronze/drop_stg_public_schools.sql"
        )



        df = pd.read_excel(
            io.BytesIO(response["Body"].read()),
            skiprows=6,
            skipfooter=1,
            sheet_name=tab_name,
            engine="openpyxl"
        )

        if len(df) == 0:
            logger.warning(f"{tab_name} can't read the data or doesn't have any data. Skipping the task.")
            return None
    
        df_long = df.melt(
            id_vars=["Region"],
            var_name="school_year",
            value_name="number_of_schools"
        )

        df_long["education_level"] = tab_name
        df_long["source_file"] = f"{bucket}/{file_name}"
        df_long = df_long.rename(columns={"Region": "region"})


        try:
            df_long.to_sql(
                name="stg_public_schools",
                con=engine,
                schema="bronze",
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )

            logger.success("Elementary data loaded successfully on the staging")


            with get_databricks_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(insert_sql)
                    logger.success("Staging data merged into the target table")

                    cursor.execute(drop_stg_sql)
                    logger.success("Staging table dropped")

        finally:
            engine.dispose() 
            response["Body"].close()

    create_table = create_table()
    elementary_data = elementary_data()
    junior_data = junior_data()
    senior_data = senior_data()

    create_table >> elementary_data >> junior_data >> senior_data



bronze_public_schools_insertions()
        







