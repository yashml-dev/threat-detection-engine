import psycopg

connection = psycopg.connect(
    host="localhost",
    port=5432,
    dbname="fraud_detection",
    user="fraud_user",
    password="fraud_password"
)

cursor = connection.cursor()

cursor.execute("SELECT current_database();")

result = cursor.fetchone()

print(result)

cursor.close()
connection.close()