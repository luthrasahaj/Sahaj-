def load_specific_file_to_sql(filename, prefix):
    import io
    import os
    import pandas as pd
    import pyodbc
    from office365.runtime.auth.authentication_context import AuthenticationContext
    from office365.sharepoint.client_context import ClientContext
    from office365.sharepoint.files.file import File
    from dotenv import load_dotenv

    # Load environment variables
    load_dotenv()

    # SharePoint + SQL credentials
    site_url = os.getenv("SHAREPOINT_SITE")
    username = os.getenv("SP_USERNAME")
    password = os.getenv("SP_PASSWORD")
    folder_url = os.getenv("SP_FOLDER_URL")
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DB")

    # Connect to SQL Server
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except pyodbc.Error as e:
        raise Exception(f"❌ Database connection failed: {e}")

    # Authenticate with SharePoint
    ctx_auth = AuthenticationContext(site_url)
    if not ctx_auth.acquire_token_for_user(username, password):
        raise Exception("❌ SharePoint authentication failed for the given user.")

    ctx = ClientContext(site_url, ctx_auth)
    folder = ctx.web.get_folder_by_server_relative_url(folder_url)
    files = folder.files
    ctx.load(files)
    ctx.execute_query()

    file = next((f for f in files if f.properties["Name"] == filename), None)
    if file is None:
        return f"❌ File '{filename}' not found in SharePoint 'api' folder."

    file_url = folder_url + "/" + filename
    response = File.open_binary(ctx, file_url)
    excel_data = pd.ExcelFile(io.BytesIO(response.content))

    for sheet_name in excel_data.sheet_names:
        print(f"ℹ️ Loading sheet '{sheet_name}'...")
        df = excel_data.parse(sheet_name)
        df.columns = [str(col).strip().replace(" ", "_") for col in df.columns]

        table_name = f"{prefix}__{sheet_name}".replace(" ", "_").replace("-", "_")
        print(f"   ⮕ Table name: {table_name}")

        # Drop old table if it exists
        cursor.execute(
            f"IF OBJECT_ID(N'{table_name}', N'U') IS NOT NULL DROP TABLE {table_name}"
        )
        conn.commit()

        # Create new table
        columns = ", ".join(f"[{col}] NVARCHAR(MAX)" for col in df.columns)
        cursor.execute(f"CREATE TABLE {table_name} ({columns})")
        conn.commit()

        # Insert data using executemany
        if not df.empty:
            cleaned_data = [
                tuple(str(val).replace("'", "''") if not pd.isna(val) else None for val in row)
                for row in df.itertuples(index=False, name=None)
            ]
            placeholders = ", ".join("?" for _ in df.columns)
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            cursor.executemany(insert_query, cleaned_data)
            conn.commit()

        print(f"✅ Sheet '{sheet_name}' successfully loaded into '{table_name}'")

    cursor.close()
    conn.close()
    return f"✅ File '{filename}' with all sheets ingested using prefix '{prefix}'."
