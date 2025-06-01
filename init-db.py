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

house_id = 3538

def create_server_connection():
    """
    Connects to the MySQL server. Does not select a database initially.
    Used for creating the database if it doesn't exist.
    """
    print(f"Connecting to MySQL server ")
    connection = None
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            port=DB_PORT,
            password=DB_PASSWORD,
            connection_timeout=10 # Set a timeout for the connection
        )
        if connection.is_connected():
            print(f"Successfully connected to MySQL server ({DB_HOST}).")
        return connection
    except Error as e:
        print(f"Error connecting to MySQL server: {e}")
        return None
    except Exception as e:
        print(f"[DEBUG] Unexpected exception: {e}")
        return None

def create_db_and_get_connection():
    """
    Ensures the database exists and returns a connection to it.
    """
    server_conn = create_server_connection()
    if not server_conn:
        print("Failed to connect to the MySQL server. Cannot create or connect to the database.")
        return None

    try:
        cursor = server_conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` DEFAULT CHARACTER SET 'utf8mb4'")
        print(f"Database '{DB_NAME}' ensured to exist.")
        cursor.close()
        # Close server-only connection and reconnect to the specific database
        server_conn.close()

        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            connection_timeout=10  # Set a timeout for the connection
        )
        if conn.is_connected():
            print(f"Successfully connected to database '{DB_NAME}'.")
        return conn
    except Error as e:
        print(f"Error creating/connecting to database '{DB_NAME}': {e}")
        if server_conn and server_conn.is_connected():
            server_conn.close()
        return None
    except Exception as e:
        print(f"[DEBUG] Unexpected exception: {e}")
        return None

def create_users_table(conn):
    """
    Creates a 'users' table in the database with basic user fields.
    """
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS `users` (
            `id` INT AUTO_INCREMENT PRIMARY KEY,
            `username` VARCHAR(50) NOT NULL UNIQUE,
            `email` VARCHAR(100) NOT NULL UNIQUE,
            `password` VARCHAR(255) NOT NULL,
            `address` VARCHAR(255) NOT NULL,
            `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("User table created successfully or already exists.")
        # Optionally, insert a default user (if needed)
        cursor.execute("INSERT INTO users (username, email, password, address) VALUES (%s, %s, %s, %s)", ('admin', 'admin@gmail.com', 'admin123', '123 Admin St'))
        conn.commit()
        print("Default user inserted into the user table.")
        cursor.close()
    except Error as e:
        print(f"Error creating user table: {e}")

def create_houses_table(conn):
    """
    Creates a 'houses' table in the database with basic user fields.
    """
    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS `houses` (
            `id` INT NOT NULL,
            `user_id` INT NOT NULL,
            `construction_year` INT,
            `total_square_footage` FLOAT,
            `first_floor_square_footage` FLOAT,
            `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, user_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Houses table created successfully or already exists.")
        # Optionally, insert a default house (if needed)
        #cursor.execute("INSERT INTO houses (id, user_id, construction_year) VALUES (%s, %s, %s)", (house_id, 1, 1999))
        #conn.commit()
        #print("Default house inserted into the houses table.")
        cursor.close()
    except Error as e:
        print(f"Error creating houses table: {e}")

def create_houses_consumption_table(cnx):
    try:
        TABLE_NAME = 'houses_consumption'
        cursor = cnx.cursor()
        print(f"Successfully connected to database '{DB_NAME}'.")

        # Define the table creation SQL statement
        # Note: DECIMAL(precision, scale) where precision is total digits, scale is digits after decimal point.
        # BOOLEAN is stored as TINYINT(1) in MySQL.
        # Assuming users.house_id is INT.
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date_time DATETIME NOT NULL,
            house_id INT NOT NULL,
            bathroom1 DECIMAL(10, 4),
            bedroom1 DECIMAL(10, 4),
            bedroom2 DECIMAL(10, 4),
            clotheswasher1 DECIMAL(10, 4),
            livingroom1 DECIMAL(10, 4),
            dishwasher1 DECIMAL(10, 4),
            garage1 DECIMAL(10, 4),
            kitchen1 DECIMAL(10, 4),
            kitchenapp1 DECIMAL(10, 4),
            kitchenapp2 DECIMAL(10, 4),
            lights_plugs1 DECIMAL(10, 4),
            lights_plugs2 DECIMAL(10, 4),
            lights_plugs3 DECIMAL(10, 4),
            microwave1 DECIMAL(10, 4),
            office1 DECIMAL(10, 4),
            range1 DECIMAL(10, 4),
            refrigerator1 DECIMAL(10, 4),
            venthood1 DECIMAL(10, 4),
            oven1 DECIMAL(10, 4),
            total_energy DECIMAL(12, 4),
            Weekday TINYINT UNSIGNED,
            Month TINYINT UNSIGNED,
            Hour TINYINT UNSIGNED,
            Hour_sin FLOAT,
            Hour_cos FLOAT,
            DoW_sin FLOAT,
            DoW_cos FLOAT,
            bathroom1_present BOOLEAN,
            bedroom1_present BOOLEAN,
            bedroom2_present BOOLEAN,
            clotheswasher1_present BOOLEAN,
            livingroom1_present BOOLEAN,
            dishwasher1_present BOOLEAN,
            garage1_present BOOLEAN,
            kitchen1_present BOOLEAN,
            kitchenapp1_present BOOLEAN,
            kitchenapp2_present BOOLEAN,
            lights_plugs1_present BOOLEAN,
            lights_plugs2_present BOOLEAN,
            lights_plugs3_present BOOLEAN,
            microwave1_present BOOLEAN,
            office1_present BOOLEAN,
            range1_present BOOLEAN,
            refrigerator1_present BOOLEAN,
            venthood1_present BOOLEAN,
            oven1_present BOOLEAN,
            PRIMARY KEY (date_time, house_id),
            FOREIGN KEY (house_id) REFERENCES houses(id)
        ) ENGINE=InnoDB;
        """

        # Execute the SQL statement
        cursor.execute(create_table_query)
        print(f"Table '{TABLE_NAME}' created successfully or already exists.")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Database '{DB_NAME}' does not exist")
        elif err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print(f"Table '{TABLE_NAME}' already exists.")
        elif err.errno == errorcode.ER_NO_REFERENCED_ROW_2 or err.errno == errorcode.ER_CANNOT_ADD_FOREIGN:
            print(f"Error creating foreign key: Ensure 'users' table exists and 'house_id' is a key in it.")
            print(f"MySQL Error: {err}")
        else:
            print(f"MySQL Error: {err}")
    finally:
      cursor.close()
            

    try:
        df = pd.read_csv(csv_file_path)
        if df.empty:
            print(f"Warning: CSV file '{csv_file_path}' is empty. Table schema might be incomplete if only headers exist.")
            # If only headers exist and no data, pandas might not infer types well.
            # We'll proceed assuming headers are present.
            if not list(df.columns):
                 print(f"Error: CSV file '{csv_file_path}' has no headers. Cannot create table.")
                 return False


        # Sanitize column headers to be valid SQL column names
        # and determine SQL types
        column_definitions = ["`id` INT AUTO_INCREMENT PRIMARY KEY"]  # Add fixed columns first
        # Store original to sanitized mapping for data insertion
        
        global column_name_mapping
        column_name_mapping = {}


        for header in df.columns:
            original_header = header
            safe_header = "".join(c if c.isalnum() else '_' for c in str(header))
            # Avoid 'id' conflict if CSV has an 'id' column
            if safe_header.lower() == 'id':
                safe_header = f"{safe_header}_csv"
            
            column_name_mapping[original_header] = safe_header

            # Basic type inference from pandas DataFrame
            # Ensure data exists for type inference, otherwise default to VARCHAR
            sql_type = "VARCHAR(255)" # Default
            if not df[original_header].empty:
                col_dtype = df[original_header].dtype
                if pd.api.types.is_integer_dtype(col_dtype):
                    sql_type = "BIGINT" # Use BIGINT for wider integer range
                elif pd.api.types.is_float_dtype(col_dtype):
                    sql_type = "DOUBLE" # Use DOUBLE for floating point numbers
                elif pd.api.types.is_bool_dtype(col_dtype):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(col_dtype):
                    sql_type = "DATETIME"
                # Add more specific types if needed (e.g., TEXT for very long strings)
            
            column_definitions.append(f"`{safe_header}` {sql_type}")

        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(column_definitions)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
        
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print(f"Table '{table_name}' created successfully or already exists.")
        cursor.close()
        return True
    except FileNotFoundError:
        print(f"Error: CSV file '{csv_file_path}' not found.")
        return False
    except pd.errors.EmptyDataError:
        print(f"Error: CSV file '{csv_file_path}' is empty (no data and no headers). Cannot create table.")
        return False
    except Error as e:
        print(f"Error creating table '{table_name}': {e}")
        return False
    except Exception as ex:
        print(f"An unexpected error occurred during table creation: {ex}")
        return False

if __name__ == "__main__":
    cnn = None # Initialize cnn to None
    try:
        # Initial check on loaded environment variables
        if not DB_HOST or not DB_USER or not DB_PASSWORD:
            print("CRITICAL ERROR: One or more required environment variables (DB_HOST, DB_USER, DB_PASSWORD) are empty or not set.")
            print("Please check your .env file and ensure it's correct.")
            exit(1) # Explicit exit if core vars are missing

        cnn = create_db_and_get_connection()
        if cnn is None:
            print("CRITICAL FAILURE: Failed to establish a database connection after multiple attempts. Exiting script.")
            exit(1) # Explicitly exit if connection could not be established
        else:
            print("INFO: Connection established. Proceeding with table creation.")
            create_users_table(cnn)
            create_houses_table(cnn)
            create_houses_consumption_table(cnn)
            


    except Exception as main_ex:
        print(f"CRITICAL ERROR: An unhandled exception occurred in the main block: {main_ex}")
        exit(1) # Catch any unexpected errors that bypass other try/excepts
    finally:
        if cnn and cnn.is_connected():
            cnn.close()
            print("INFO: MySQL connection is closed.")
        else:
            print("INFO: No active MySQL connection to close or connection was already closed.")

