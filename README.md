# BEMS App Backend Server

This is a Flask-based backend API for managing and analyzing energy consumption data. It uses a MySQL database and supports CSV-based data ingestion.


## Prerequisites

1. **MySQL Database**  
  Ensure you have a MySQL database installed and running.

2. **Environment Variables**  
  Create a `.env` file in the project root with the following structure:

  ```env
  DB_HOST=127.0.0.1
  DB_PORT=3306
  DB_USER=your_db_username
  DB_PASSWORD=your_password
  DB_NAME=bems_db
  CSV_FILE_PATH=../data/
  ```
  - Replace the values as needed.
  - `CSV_FILE_PATH` should point to the directory containing your CSV data.

3. **Install Dependencies**  
  It is recommended to use a virtual environment. Install the required Python modules:
  ```bash
  python -m venv venv
  source venv/bin/activate   # On Windows: venv\Scripts\activate
  ```

  ```bash
  pip install Flask Flask-MySQLdb Flask-Cors python-dotenv pandas mysql-connector-python pyjwt
  ```

## Setup

4. **Initialize Database Tables**  
  Run the following script to create the necessary tables. Make sure to set the correct house ID in the script:

  ```bash
  python init-db.py
  ```

5. **Seed Database**  
  Populate the `houses_consumption` table with your CSV data:

  ```bash
  python seed-db.py
  ```

## Running the Server

6. **Start the API Server**  
  Launch the backend server:

  ```bash
  python server-api.py
  ```

---

Feel free to open issues or contribute to this repository!