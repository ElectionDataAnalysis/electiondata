#!usr/bin/python

from flask import Flask
import psycopg2


host_name = 'db'
user_name = 'postgres'
password = 'notverysecure'
database = 'tmp'

def establish_connection():
    print ("establishing connection to database...")
    # the connect() function returns a new instance of connection
    conn = psycopg2.connect(host = host_name, user = user_name, password = password, dbname = database)
    return conn

def create_cursor(connection):
    # create a new cursor with the connection object.
    cur = connection.cursor()
    get_version(cur)
    return cur

def get_version(cur):
    cur.execute('SELECT version()')
    version = cur.fetchone()
    print ("PostgreSQL Version : ", version)

def create_tables(cur):
    tables = ("""CREATE TABLE Agents (
                agent_id INTEGER PRIMARY KEY,
                first_name VARCHAR(255) NOT NULL,
                last_name VARCHAR(255) NOT NULL,
                country VARCHAR(255) NOT NULL,
                salary INTEGER NOT NULL,
                born DATE NOT NULL,
                dept_id INTEGER NOT NULL,
                mission_id INTEGER NOT NULL
            )""",
            """CREATE TABLE Mission (
                mission_id INTEGER PRIMARY KEY,
                mission_name VARCHAR NOT NULL,
                location VARCHAR NOT NULL
            )""",
            """CREATE TABLE Department (
                dept_id INTEGER PRIMARY KEY,
                dept_name VARCHAR(255)
            )""")

    for table in tables:
        cur.execute(table)

def insert(cur):
    entry = "INSERT INTO Agents VALUES{}".format("(110454, john, willis, june 14 1989, 6247, 119)")
    cur.execute(entry)
    #for insert multiple rows it will be cur.executemany()


def update(cur):
    update_entry = """UPDATE Agents SET last_name = %s WHERE agent_id = %s"""
    cur.execute(update_entry, william, 110454)
    no_of_rows_updated = cur.rowcount


def query(cur):
    result = cur.execute("SELECT first_name FROM Agent")
    one_entry = cur.fecthone()
    all_entries = cur.fetchall()
    for entry in all_ertries:
        print (entry)
        

app = Flask(__name__)

@app.route('/')
def hello():
    print("Connecting to db")
    db_connection = establish_connection()
    connection_cursor = create_cursor(db_connection)

    print ("Creating Tables: ")
    create_tables(connection_cursor)

    db_connection.commit()

    if connection_cursor is not None:
        connection_cursor.close()
    if db_connection is not None:
        db_connection.close()
    return 'Hello Beautiful World!\n'
