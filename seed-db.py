import pandas as pd
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')  # Replace with your MySQL username
DB_PASSWORD = os.getenv('DB_PASSWORD')  # Replace with your MySQL password
DB_NAME = os.getenv('DB_NAME','bems_db')  # Name of the database to create/use
DB_PORT = int(os.getenv('DB_PORT', 3306))  # Default MySQL port is 3306
#TABLE_NAME = os.environ.get('TABLE_NAME', 'items')  # Default table name for CSV data
CSV_FILE_PATH = os.getenv('CSV_FILE_PATH')  # Path to your CSV file

print(f"\n--- Environment Variables as seen by script ---")
print(f"DB_HOST: '{DB_HOST}'")
print(f"DB_USER: '{DB_USER}'")
print(f"DB_PASSWORD: '{DB_PASSWORD}'") # For debugging, this prints the actual password
print(f"DB_NAME: '{DB_NAME}'")
print(f"DB_PORT: {DB_PORT}")
print(f"CSV_FILE_PATH: '{CSV_FILE_PATH}'")
print(f"---------------------------------------------\n")

file_path = CSV_FILE_PATH+ 'house_3538.csv'
house_id = 3538

def load_data():
    # Load the dataset
    df = pd.read_csv(file_path)  

    
    df = df.drop(['dataid', 'house_construction_year','total_square_footage', 'first_floor_square_footage' ], axis=1, errors='ignore')  # Drop columns that are not needed
    df.rename(columns={'local_15min': 'date_time'}, inplace=True)

    # Add the house_id column
    df['house_id'] = house_id

    # Convert 'date_time' to datetime objects to ensure correct formatting for MySQL
    df['date_time'] = pd.to_datetime(df['date_time'])
    
    # Convert all columns to string type
    #df = df.astype(str)
    
    # Set 'date' as the index
    #df.set_index('date', inplace=True)

    # Sort the DataFrame by index
    #df.sort_index(inplace=True)

    return df

def load_csv_data_to_mysql(conn, table_name):
    """
    Loads data from the CSV file into the specified MySQL table.
    Assumes table structure matches CSV headers (after sanitization).
    Skips loading if the table already contains data.
    """
    try:
        df = load_data() # Read all as string initially to avoid pandas type issues with N/A
        if df.empty:
            print(f"CSV file '{file_path}' is empty. No data to load.")
            return 0

        cursor = conn.cursor()

                # Prepare the SQL INSERT statement
        # Ensure the order of columns in the INSERT statement matches your DataFrame columns
        # The list of columns from your table definition
        table_columns = [
            'date_time', 'house_id', 'bathroom1', 'bedroom1', 'bedroom2',
            'clotheswasher1', 'livingroom1', 'dishwasher1', 'garage1', 'kitchen1',
            'kitchenapp1', 'kitchenapp2', 'lights_plugs1', 'lights_plugs2',
            'lights_plugs3', 'microwave1', 'office1', 'range1', 'refrigerator1',
            'venthood1', 'oven1', 'total_energy', 'Weekday', 'Month', 'Hour',
            'Hour_sin', 'Hour_cos', 'DoW_sin', 'DoW_cos',
            'bathroom1_present', 'bedroom1_present', 'bedroom2_present',
            'clotheswasher1_present', 'livingroom1_present', 'dishwasher1_present',
            'garage1_present', 'kitchen1_present', 'kitchenapp1_present',
            'kitchenapp2_present', 'lights_plugs1_present', 'lights_plugs2_present',
            'lights_plugs3_present', 'microwave1_present', 'office1_present',
            'range1_present', 'refrigerator1_present', 'venthood1_present',
            'oven1_present'
        ]

        # Make sure the DataFrame columns are in the same order as your table columns
        df = df[table_columns]

        # Create a placeholder string for the SQL query
        placeholders = ', '.join(['%s'] * len(table_columns))
        insert_query = f"INSERT INTO {table_name} ({', '.join(table_columns)}) VALUES ({placeholders})"

        # Convert DataFrame to a list of tuples for batch insertion
        data_to_insert = [tuple(row) for row in df.itertuples(index=False)]

        # Execute the bulk insert
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} rows into the table.")

    except Error as e:
        print(f"Error while connecting to MySQL or inserting data: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")
    

if __name__ == "__main__":
    try:
        # Connect to MySQL database
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )

        if conn.is_connected():
            print(f"Connected to MySQL database '{DB_NAME}' at {DB_HOST}:{DB_PORT} as user '{DB_USER}'.")

            # Load data from CSV and insert into MySQL table
            load_csv_data_to_mysql(conn,'houses_consumption')

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("MySQL connection closed.")