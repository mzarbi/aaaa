import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="taskqueue",
    password="taskqueue",
    database="taskqueue"
)
conn.autocommit = True