import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

server = os.getenv("SQL_SERVER")
database = os.getenv("SQL_DB")

conn_str = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
)

def sanitize_table_name(sheet_name, file_name):
    file_base = os.path.splitext(os.path.basename(file_name))[0]
    return f"{file_base}_{sheet_name}".replace(" ", "_").replace("-", "_")

def table_exists(cursor, table_name):
    cursor.execute("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?", table_name)
    return cursor.fetchone() is not None

def load_to_sql(file_path):
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    if file_path.endswith(".csv"):
        df = pd.read_csv(file_path)
        table_name = os.path.splitext(os.path.basename(file_path))[0].replace(" ", "_")
        if table_exists(cursor, table_name):
            print(f"🔁 Skipping existing table: {table_name}")
            return
        insert_dataframe(df, table_name, cursor)

    elif file_path.endswith(".xlsx"):
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            df = xls.parse(sheet_name)
            table_name = sanitize_table_name(sheet_name, file_path)
            if table_exists(cursor, table_name):
                print(f"🔁 Skipping existing table: {table_name}")
                continue
            insert_dataframe(df, table_name, cursor)

    conn.commit()
    conn.close()

def insert_dataframe(df, table_name, cursor):
    df = df.dropna(how='all')  # remove empty rows
    df.columns = [col.strip().replace(" ", "_") for col in df.columns]

    create_cols = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
    cursor.execute(f"CREATE TABLE [{table_name}] ({create_cols})")

    for _, row in df.iterrows():
        cleaned_row = [str(cell) if pd.notnull(cell) else None for cell in row]
        placeholders = ", ".join(["?" for _ in cleaned_row])
        insert_query = f"INSERT INTO [{table_name}] VALUES ({placeholders})"
        try:
            cursor.execute(insert_query, cleaned_row)
        except Exception as e:
            print(f"⚠️ Failed to insert row into {table_name}: {e}")
