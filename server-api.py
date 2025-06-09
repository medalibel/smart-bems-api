from flask import Flask, jsonify, request, Response, send_file
from flask_mysqldb import MySQL
from flask_cors import CORS
import jwt
import os
import io
import csv
from dotenv import load_dotenv
from datetime import datetime,timedelta
from decimal import Decimal
from datetime import date


load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')  # Replace with your MySQL username
DB_PASSWORD = os.getenv('DB_PASSWORD')  # Replace with your MySQL password
DB_NAME = os.getenv('DB_NAME','bems_db')  # Name of the database to create/use
DB_PORT = int(os.getenv('DB_PORT', 3306)) 
SECRET_KEY = os.getenv('SECRET_KEY', 'your_secret_key')  # Replace with your secret key for JWT

# yyyymmdd hh:mm:ss format
# Default date for testing purposes
DATE_TODAY = datetime(2025,6,1,12,0,0) #'2025-06-01 12:00:00'  # Example date, adjust as needed

#SELECT date_time, house_id, total_energy
#FROM houses_consumption
#WHERE date_time BETWEEN '2025-01-01 00:00:00' AND '2025-01-01 23:59:59'
#AND house_id = 3538;

app = Flask(__name__)
app.config['SECRET_KEY'] = SECRET_KEY
CORS(app,expose_headers=["Content-Disposition"],supports_credentials=True) # This will enable CORS for all routes

# --- MySQL Configuration ---
# Replace with your actual MySQL connection details
app.config['MYSQL_HOST'] = DB_HOST
app.config['MYSQL_USER'] = DB_USER
app.config['MYSQL_PASSWORD'] = DB_PASSWORD
app.config['MYSQL_DB'] = DB_NAME
app.config['MYSQL_CURSORCLASS'] = 'DictCursor' # Returns results as dictionaries

mysql = MySQL(app)

# --- Helper Function to Execute Queries ---
def execute_query(query, args=None, fetchone=False, commit=False):
    """
    Executes a SQL query and returns the result.
    """
    cur = mysql.connection.cursor()
    cur.execute(query, args)
    if commit:
        mysql.connection.commit()
        cur.close()
        return None # Or return lastrowid, rowcount etc. if needed
    result = cur.fetchone() if fetchone else cur.fetchall()
    cur.close()
    return result

# --- Routes 

@app.route('/login', methods=['POST'])
def login():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    try:
        if not email or not password:
            return jsonify({'error': 'Username and password are required.'}), 400
        user = execute_query("SELECT * FROM users WHERE email = %s", (email,),fetchone=True)
        if not user:
            print(f"User not found for email: {email}")
            return jsonify({'error': 'Invalid credentials.'}), 401
        username = user['username']
        id = user['id']
        address = user['address']
        if not user['password'] == password:
            print(f"Invalid password for user: {username}")
            return jsonify({'error': 'Invalid credentials.'}), 401
        
        token = jwt.encode({
            'username': username,
            'id': id,
            'address': address,
            'exp': datetime.now() + timedelta(hours=1)
        }, app.config['SECRET_KEY'], algorithm='HS256')
        print(f"user logged in {username}: {token}")
        return jsonify({'token': token, 'username': username, 'id': id, 'address': address}), 200
    except Exception as e:
        print(f"Error during login: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/consumption/today', methods=['GET'])
def get_today_data():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({'error': 'Missing token'}), 401

    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        print(decoded)
        user_id = decoded['id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    startDate = datetime(DATE_TODAY.year, DATE_TODAY.month, DATE_TODAY.day, 0, 0, 0)
    try:
        house = execute_query("SELECT * FROM houses WHERE user_id = %s", (user_id,),fetchone=True)
        items = execute_query(f"""SELECT 
            date_time,
            house_id,
            bathroom1,
            bedroom1 ,
            bedroom2 ,
            clotheswasher1 ,
            livingroom1 ,
            dishwasher1 ,
            garage1 ,
            kitchen1 ,
            kitchenapp1 ,
            kitchenapp2 ,
            lights_plugs1 ,
            lights_plugs2 ,
            lights_plugs3 ,
            microwave1 ,
            office1 ,
            range1 ,
            refrigerator1 ,
            venthood1 ,
            oven1,
            total_energy FROM houses_consumption WHERE house_id = %s AND date_time BETWEEN %s AND %s""",
            (house['id'],startDate, DATE_TODAY), fetchone=False)
        return jsonify(items), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/consumption/lastweek', methods=['GET'])
def get_weekly_totals():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({'error': 'Missing token'}), 401

    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        print(decoded)
        user_id = decoded['id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    startDate = DATE_TODAY - timedelta(days=7)
    startDate = datetime(startDate.year, startDate.month, startDate.day, 0, 0, 0)
    try:
        house = execute_query("SELECT * FROM houses WHERE user_id = %s", (user_id,),fetchone=True)
        items = execute_query("""
            SELECT 
                DATE(date_time) as day,
                SUM(bathroom1) as bathroom1,
                SUM(bedroom1) as bedroom1,
                SUM(bedroom2) as bedroom2,
                SUM(livingroom1) as livingroom1,
                SUM(garage1) as garage1,
                SUM(kitchen1) as kitchen1,
                SUM(office1) as office1,
                SUM(range1) as range1,
                SUM(venthood1) as venthood1,
                SUM(total_energy) as total_consumption
            FROM houses_consumption
            WHERE house_id = %s AND date_time BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day ASC
        """, (house['id'],startDate, DATE_TODAY), fetchone=False)
        return jsonify(items), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/consumption/<string:start>/<string:end>', methods=['GET'])
def get_range_total(start, end):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({'error': 'Missing token'}), 401

    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        print(decoded)
        user_id = decoded['id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    #startDate = datetime(DATE_TODAY.year, DATE_TODAY.month, DATE_TODAY.day, 0, 0, 0)
    query = request.args.get('preset', 'N/A')
    print(f"Query preset: {query}")
    format_string = "%Y-%m-%d"

    try:
        if query != 'N/A' and query.isdigit():
            parsed_startdate = DATE_TODAY - timedelta(days=int(query))
            parsed_startdate = datetime(parsed_startdate.year, parsed_startdate.month, parsed_startdate.day, 0, 0, 0)
            parsed_enddate = DATE_TODAY
        else:
            # strptime by default creates a datetime object with time components set to 00:00:00
            parsed_startdate = datetime.strptime(start, format_string)
            parsed_enddate = datetime.strptime(end, format_string)
            
            print(f"Parsed datetime object: {parsed_startdate}")
            print(f"Parsed datetime object: {parsed_enddate}")

            startDate = parsed_startdate.date()
            endDate = parsed_enddate.date()

            if startDate > endDate:
                return jsonify({'error': 'Start date cannot be after end date.'}), 400
            if startDate > DATE_TODAY.date():
                return jsonify({'error': 'Start date cannot be in the future.'}), 400
            if endDate > DATE_TODAY.date():
                return jsonify({'error': 'End date cannot be in the future.'}), 400
            if startDate == endDate:
                parsed_enddate = datetime(endDate.year, endDate.month, endDate.day, 23, 59, 59)
            if endDate == DATE_TODAY.date():
                parsed_enddate = DATE_TODAY

    except ValueError as e:
        print(f"Error parsing date string '{start}': {e}")
        print(f"Ensure the string exactly matches the format '{format_string}'.")
        return jsonify({'error': f"Ensure the string exactly matches the format '{format_string}'."}), 500
    try:
        house = execute_query("SELECT * FROM houses WHERE user_id = %s", (user_id,),fetchone=True)
        items = execute_query(f"""
            SELECT 
                DATE(date_time) as day,
                SUM(bathroom1) as bathroom1,
                SUM(bedroom1) as bedroom1,
                SUM(bedroom2) as bedroom2,
                SUM(livingroom1) as livingroom1,
                SUM(garage1) as garage1,
                SUM(kitchen1) as kitchen1,
                SUM(office1) as office1,
                SUM(range1) as range1,
                SUM(venthood1) as venthood1,
                SUM(total_energy) as total_consumption
            FROM houses_consumption
            WHERE house_id = %s AND date_time BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day ASC""",
            (house['id'],parsed_startdate, parsed_enddate), fetchone=False)
        return jsonify(items), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/consumption/download/<string:start>/<string:end>', methods=['GET'])
def download_consumption_data(start, end):

    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({'error': 'Missing token'}), 401

    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        print(decoded)
        user_id = decoded['id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    format_string = "%Y-%m-%d"
    try:
        
        # strptime by default creates a datetime object with time components set to 00:00:00
        parsed_startdate = datetime.strptime(start, format_string)
        parsed_enddate = datetime.strptime(end, format_string)
        
        print(f"Parsed datetime object: {parsed_startdate}")
        print(f"Parsed datetime object: {parsed_enddate}")

        startDate = parsed_startdate.date()
        endDate = parsed_enddate.date()

        if startDate > endDate:
            return jsonify({'error': 'Start date cannot be after end date.'}), 400
        if startDate > DATE_TODAY.date():
            return jsonify({'error': 'Start date cannot be in the future.'}), 400
        if endDate > DATE_TODAY.date():
            return jsonify({'error': 'End date cannot be in the future.'}), 400
        if startDate == endDate:
            parsed_enddate = datetime(endDate.year, endDate.month, endDate.day, 23, 59, 59)
        if endDate == DATE_TODAY.date():
            parsed_enddate = DATE_TODAY

    except ValueError as e:
        print(f"Error parsing date string '{start}': {e}")
        print(f"Ensure the string exactly matches the format '{format_string}'.")
        return jsonify({'error': f"Ensure the string exactly matches the format '{format_string}'."}), 500
    try:
        house = execute_query("SELECT * FROM houses WHERE user_id = %s", (user_id,),fetchone=True)
        query = f"""
            SELECT 
                DATE(date_time) as day,
                SUM(bathroom1) as bathroom1,
                SUM(bedroom1) as bedroom1,
                SUM(bedroom2) as bedroom2,
                SUM(livingroom1) as livingroom1,
                SUM(garage1) as garage1,
                SUM(kitchen1) as kitchen1,
                SUM(office1) as office1,
                SUM(range1) as range1,
                SUM(venthood1) as venthood1,
                SUM(total_energy) as total_consumption
            FROM houses_consumption
            WHERE house_id = %s AND date_time BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day ASC"""
        cur = mysql.connection.cursor()
        cur.execute(query, (house['id'],parsed_startdate, parsed_enddate))
        result = cur.fetchall()
        # Get column names for dictionary formatting
        columns = [desc[0] for desc in cur.description]
        cur.close()
        print("Raw DB result:", result)
        print("Columns:", columns)
        # Format as a list of dictionaries if not fetchone
        
        items = []

        for row in result:
            item = {key: str(value) if isinstance(value, Decimal) else value for key, value in row.items()}
            
            #if isinstance(item['day'], date):  # You can also import datetime.date to check this
               # item['day'] = item['day'].strftime('%Y-%m-%d')
            
            items.append(item)
        if not items:
            return jsonify({"message": "No data found for the specified date range."}), 404

        # Create a CSV in memory
        print(items)
        output = io.StringIO()
        fieldnames = [
            "day", "bathroom1", "bedroom1", "bedroom2", "livingroom1",
            "garage1", "kitchen1", "office1", "range1", "venthood1", "total_consumption"
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)

        writer.writeheader()
        for item in items:
            # Format the 'day' field to a more standard date format if needed
            #if isinstance(item['day'], datetime):
             #   item['day'] = item['day'].strftime('%Y-%m-%d')
            writer.writerow(item)

        output.seek(0) # Go to the beginning of the stream

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": "attachment;filename=consumption_data.csv"
            }
        )

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/consumption/quarter/<int:quarter>/<int:year>', methods=['GET'])
def get_quarter_data(quarter, year):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({'error': 'Missing token'}), 401

    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        print(decoded)
        user_id = decoded['id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    if quarter == 1:
        start = datetime(year,1,1,0,0,0)
        end = datetime(year,3,31,23,59,59)
    elif quarter == 2:
        start = datetime(year,4,1,0,0,0)
        end = datetime(year,6,30,23,59,59)
    elif quarter == 3:
        start = datetime(year,7,1,0,0,0)
        end = datetime(year,9,30,23,59,59)
    elif quarter == 4:
        start = datetime(year,10,1,0,0,0)
        end = datetime(year,12,31,23,59,59)
    else:
        return jsonify({'error': 'Invalid quarter.'}), 400
    
    if start > DATE_TODAY:
        return jsonify({'error': 'no available data yet'}), 400
    if end > DATE_TODAY:
        end = DATE_TODAY
    
    try:
        house = execute_query("SELECT * FROM houses WHERE user_id = %s", (user_id,),fetchone=True)
        items = execute_query(f"""
            SELECT 
                DATE(date_time) as day,
                SUM(bathroom1) as bathroom1,
                SUM(bedroom1) as bedroom1,
                SUM(bedroom2) as bedroom2,
                SUM(livingroom1) as livingroom1,
                SUM(garage1) as garage1,
                SUM(kitchen1) as kitchen1,
                SUM(office1) as office1,
                SUM(range1) as range1,
                SUM(venthood1) as venthood1,
                SUM(total_energy) as total_consumption
            FROM houses_consumption
            WHERE house_id = %s AND date_time BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day ASC""",
            (house['id'],start, end), fetchone=False)
        return jsonify(items), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/bills/download/<int:quarter>/<int:year>', methods=['GET'])
def download_bill_data(quarter, year):
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({'error': 'Missing token'}), 401

    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        print(decoded)
        user_id = decoded['id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401

    if quarter == 1:
        start = datetime(year,1,1,0,0,0)
        end = datetime(year,3,31,23,59,59)
    elif quarter == 2:
        start = datetime(year,4,1,0,0,0)
        end = datetime(year,6,30,23,59,59)
    elif quarter == 3:
        start = datetime(year,7,1,0,0,0)
        end = datetime(year,9,30,23,59,59)
    elif quarter == 4:
        start = datetime(year,10,1,0,0,0)
        end = datetime(year,12,31,23,59,59)
    else:
        return jsonify({'error': 'Invalid quarter.'}), 400
    
    if start > DATE_TODAY:
        return jsonify({'error': 'no available data yet'}), 400
    if end > DATE_TODAY:
        end = DATE_TODAY
    
    try:
        house = execute_query("SELECT * FROM houses WHERE user_id = %s", (user_id,),fetchone=True)
        query = f"""
            SELECT 
                DATE(date_time) as day,
                SUM(bathroom1) as bathroom1,
                SUM(bedroom1) as bedroom1,
                SUM(bedroom2) as bedroom2,
                SUM(livingroom1) as livingroom1,
                SUM(garage1) as garage1,
                SUM(kitchen1) as kitchen1,
                SUM(office1) as office1,
                SUM(range1) as range1,
                SUM(venthood1) as venthood1,
                SUM(total_energy) as total_consumption
            FROM houses_consumption
            WHERE house_id = %s AND date_time BETWEEN %s AND %s
            GROUP BY day
            ORDER BY day ASC"""
        cur = mysql.connection.cursor()
        cur.execute(query, (house['id'],start, end))
        result = cur.fetchall()
        # Get column names for dictionary formatting
        columns = [desc[0] for desc in cur.description]
        cur.close()
        print("Raw DB result:", result)
        print("Columns:", columns)
        # Format as a list of dictionaries if not fetchone
        
        items = []

        for row in result:
            item = {key: str(value) if isinstance(value, Decimal) else value for key, value in row.items()}
            
            #if isinstance(item['day'], date):  # You can also import datetime.date to check this
               # item['day'] = item['day'].strftime('%Y-%m-%d')
            
            items.append(item)
        if not items:
            return jsonify({"message": "No data found for the specified date range."}), 404

        # Create a CSV in memory
        print(items)
        output = io.StringIO()
        fieldnames = [
            "day", "bathroom1", "bedroom1", "bedroom2", "livingroom1",
            "garage1", "kitchen1", "office1", "range1", "venthood1", "total_consumption"
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)

        writer.writeheader()
        for item in items:
            # Format the 'day' field to a more standard date format if needed
            #if isinstance(item['day'], datetime):
             #   item['day'] = item['day'].strftime('%Y-%m-%d')
            writer.writerow(item)

        output.seek(0) # Go to the beginning of the stream

        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={
                "Content-Disposition": "attachment;filename=consumption_data.csv"
            }
        )
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/bills', methods=['GET'])
def get_bills_data():
    
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith("Bearer "):
        return jsonify({'error': 'Missing token'}), 401

    token = auth_header.split(" ")[1]
    try:
        decoded = jwt.decode(token, app.config['SECRET_KEY'], algorithms=['HS256'])
        print(decoded)
        user_id = decoded['id']
    except jwt.ExpiredSignatureError:
        return jsonify({'error': 'Token expired'}), 401
    except jwt.InvalidTokenError:
        return jsonify({'error': 'Invalid token'}), 401
    try:
        house = execute_query("SELECT * FROM houses WHERE user_id = %s", (user_id,),fetchone=True)
        items = execute_query(f"""
            SELECT
                DATE_FORMAT(date_time, '%%Y-%%m') AS month,
                SUM(total_energy) AS monthly_consumption,
                COUNT(*) AS total_records
            FROM houses_consumption
            WHERE house_id = %s AND date_time <= %s
            GROUP BY DATE_FORMAT(date_time, '%%Y-%%m')
            ORDER BY month ASC""",
            (house['id'],DATE_TODAY), fetchone=False)
        return jsonify(items), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# basic route for testing
@app.route('/')
def index():
    return "Hello, Flask with MySQL and CORS is running!"

if __name__ == '__main__':
    # Remember to set debug=False in a production environment
    app.run(debug=True, port=5001) # Running on port 5001 to avoid conflict with Next.js default port