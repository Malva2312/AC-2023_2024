import sqlite3
import pandas as pd

def create_table_from_csv(database_name, table_name, schema, csv_file):
    """
    Create a table in a SQLite database using a schema and populate it with data from a CSV file.

    Args:
        database_name (str): The name of the SQLite database.
        table_name (str): The name of the table to be created.
        schema (list of str): A list of SQL statements defining the table schema.
        csv_file (str): The path to the CSV file containing data to be inserted into the table.
    """
    # Database connection
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()

    try:
        # Create the table using the provided schema
        for statement in schema:
            cursor.execute(statement)

        # Read data from CSV file into a DataFrame
        df = pd.read_csv(csv_file)

        # Insert data into the table
        df.to_sql(table_name, conn, schema=schema, if_exists='replace', index=False)

        # Commit changes and close the connection
        conn.commit()
        conn.close()

        print(f"Table '{table_name}' created and populated with data from '{csv_file}'.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        conn.rollback()
        conn.close()