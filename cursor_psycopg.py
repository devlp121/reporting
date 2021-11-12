import psycopg2

# Connection to database server
conn = psycopg2.connect(host="localhost",database="stream",port=5432,user="postgres",password="pagerer")

# Create cursor to execute SQL commands
cur = conn.cursor()

# Execute SQL commands
for command in commands:
    # Create tables
    cur.execute(command)

# Close communication with server
conn.commit()
cur.close()
conn.close()