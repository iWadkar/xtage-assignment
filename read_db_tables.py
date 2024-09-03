import psycopg2
import pandas as pd
from config import DATABASE

def connect_to_postgres_and_read_data(query):
    """Connect to PostgreSQL and read data into a Pandas DataFrame."""
    try:
        # Connect to your PostgreSQL DB
        connection = psycopg2.connect(
            dbname=DATABASE['dbname'],
            user=DATABASE['user'],
            password=DATABASE['password'],
            host=DATABASE['host'],
            port=DATABASE['port']
        )

        # Read data from PostgreSQL table
        df = pd.read_sql(query, connection)

        # Close the connection
        connection.close()

        return df  # Return the DataFrame

    except Exception as error:
        print("Error connecting to PostgreSQL database:", error)
        return None

# Example query
query = "SELECT * FROM products;"  # Replace 'products' with your actual table name

query2 = "SELECT * FROM transactions;"

# Call the function to connect and read data

df = connect_to_postgres_and_read_data(query)
print("Data from the table:")
print(df.head())  # Print the first few rows of the DataFrame

df2 = connect_to_postgres_and_read_data(query2)
print("Data from transactions table:")
print(df2.head())

df.to_csv('db_products.csv', index=False)
df2.to_csv('db_transactions.csv', index=False)