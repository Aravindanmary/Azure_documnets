import pandas as pd
from sqlalchemy import create_engine, inspect

# The file paths for the two Excel files
file1 = "C:\\Users\\h569411\\OneDrive - Honeywell\\Documents\\Azure-delta lake\\New-demo-project\\Configuration.xlsx"
file2 = "C:\\Users\\h569411\\OneDrive - Honeywell\\Documents\\Azure-delta lake\\New-demo-project\\Value.xlsx"

# Connection string to the MSSQL database
connection_string = "mssql+pyodbc://IE7HLT7TB7S73\SQLEXPRESS/Demo-Project1?driver=ODBC Driver 17 for SQL Server"

try:
    # Create the SQLAlchemy engine
    engine = create_engine(connection_string)
    connection = engine.connect()  # Ensure the connection works
    print("Database connection was successful.")

    # Load Excel data for the two files
    data1 = pd.read_excel(file1)
    data2 = pd.read_excel(file2)

    # Define key columns for both files (ensure these are present in both DataFrames)
    key_column1 = "SNO"  # Replace with the correct key column in Configuration.xlsx
    key_column2 = "SNO"  # Replace with the correct key column in Value.xlsx

    # Inspect the database to check for existing tables
    inspector = inspect(engine)

    # Handling Configuration.xlsx
    if "Configuration" not in inspector.get_table_names():
        # If the table doesn't exist, create a new table
        data1.to_sql("Configuration", con=engine, index=False, if_exists='replace')
        print("Created a new table 'Configuration' and inserted data from Configuration.xlsx.")
    else:
        # Table exists, read data from the existing SQL table and check for new rows
        df_config_sql = pd.read_sql("SELECT * FROM Configuration", engine)
        data1[key_column1] = data1[key_column1].astype(str)
        df_config_sql[key_column1] = df_config_sql[key_column1].astype(str)
        
        # Identify new rows in Configuration.xlsx
        new_rows_config = data1[~data1[key_column1].isin(df_config_sql[key_column1])]
        
        if not new_rows_config.empty:
            # Insert new rows into the database table
            new_rows_config.to_sql("Configuration", con=engine, index=False, if_exists='append')
            print("Added new rows to the existing 'Configuration' table.")
        else:
            print("No new rows found in Configuration.xlsx.")

    # Handling Value.xlsx
    if "Value" not in inspector.get_table_names():
        # If the table doesn't exist, create a new table
        data2.to_sql("Value", con=engine, index=False, if_exists='replace')
        print("Created a new table 'Value' and inserted data from Value.xlsx.")
    else:
        # Table exists, read data from the existing SQL table and check for new rows
        df_value_sql = pd.read_sql("SELECT * FROM Value", engine)
        data2[key_column2] = data2[key_column2].astype(str)
        df_value_sql[key_column2] = df_value_sql[key_column2].astype(str)
        
        # Identify new rows in Value.xlsx
        new_rows_value = data2[~data2[key_column2].isin(df_value_sql[key_column2])]
        
        if not new_rows_value.empty:
            # Insert new rows into the database table
            new_rows_value.to_sql("Value", con=engine, index=False, if_exists='append')
            print("Added new rows to the existing 'Value' table.")
        else:
            print("No new rows found in Value.xlsx.")

except Exception as e:
    print(f"An error occurred while working with the database: {e}")