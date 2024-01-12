from mysql import connector
from configparser import ConfigParser
import os

# create config
config = ConfigParser()
config.read(f"{os.path.dirname(os.path.abspath(__file__))}/../config/config.ini")

# return mysql params
host = config.get("mysql", "host")
port = config.get("mysql", "port")
user = config.get("mysql", "user")
passwd = config.get("mysql", "passwd")

def return_conn(database:str):
    config = {
        'user': user,
        'password': passwd,
        'host': host,
        'database': database
    }
    conn = connector.connect(**config)
    
    return conn

def execute_query(database:str, query:str, values:tuple=None):
    try:
        conn = return_conn(database=database)
        cursor = conn.cursor()
        cursor.execute(query, values) if values != None \
            else cursor.execute(query)
        conn.commit()
        conn.close()

    except Exception as E:
        # need to fix <<<<<<<<<<<<<<<<<<<<<<
        print(E)

def fetchall_query(database:str, query:str, values:tuple=None):
    try:
        conn = return_conn(database=database)
        cursor = conn.cursor()
        cursor.execute(query, values) if values != None \
            else cursor.execute(query)
        fetchall = cursor.fetchall()
        conn.close()
        
        return fetchall
    
    except Exception as E:
        # need to fix <<<<<<<<<<<<<<<<<<<<<<
        print(E)