import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# Database connection details
db_config = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": 5433,
}

# Folder containing CSV files
folder_path = "data"
schema = "sap_raw"

# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_config)
cursor = conn.cursor()

try:
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            table_name = os.path.splitext(file_name)[0]  # Use file name as table name
            file_path = os.path.join(folder_path, file_name)

            # Read the CSV file to get column names
            df = pd.read_csv(file_path, nrows=0)  # Load only the headers
            columns = df.columns.tolist()

            # Create table schema with all columns as TEXT
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema}
.{table_name} (
                {', '.join([f'"{col}" TEXT' for col in columns])}
            );
            """
            cursor.execute(create_table_query)
            conn.commit()

            # Copy data into the table
            with open(file_path, "r", encoding="utf-8") as f:
                copy_query = f"""
                COPY {schema}
.{table_name} ({', '.join([f'"{col}"' for col in columns])})
                FROM STDIN
                WITH CSV HEADER;
                """
                cursor.copy_expert(copy_query, f)

            print(f"Successfully imported {file_name} into {schema}
.{table_name}")
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()
finally:
    cursor.close()
    conn.close()
