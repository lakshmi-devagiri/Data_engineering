import mysql.connector

# Connect to MySQL
conn = mysql.connector.connect(
    host='database-mysql.cn6m6kai25os.ap-south-1.rds.amazonaws.com',
    user='admin',
    password='Mypassword.1',
    database='sriramdb'
)

'''cursor = conn.cursor()
# Run a query
query = "SELECT * FROM emp WHERE sal >2000;"
cursor.execute(query)

# Fetch and print results
rows = cursor.fetchall()
from decimal import Decimal
import datetime

for row in rows:
    formatted = [str(col) if col is not None else '' for col in row]
    print(", ".join(formatted))

# Cleanup
cursor.close()
conn.close()'''
import pandas as pd
import mysql.connector
query = "SELECT * FROM emp WHERE sal >2000;"
df = pd.read_sql(query, conn)

# Convert all columns to string (including Decimal, date, etc.)
df = df.astype(str)

# Display cleanly
print(df)

conn.close()
