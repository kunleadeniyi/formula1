import requests
import json


import psycopg2
import sys
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

ENDPOINT=os.getenv('DB_ENDPOINT')
PORT=os.getenv('DB_PORT')
USER=os.getenv('DB_USER')
REGION=os.getenv('DB_REGION')
DBNAME=os.getenv('DB_NAME')
PASSWORD=os.getenv('DB_PASSWORD')

# print(ENDPOINT, PORT, USER, REGION, DBNAME)

#gets the credentials from .aws/credentials
session = boto3.Session(profile_name='default')
client = session.client('rds')

# token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

try:
    conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=PASSWORD, sslrootcert="SSLCERTIFICATE")
    cur = conn.cursor()
    cur.execute("""SELECT now()""")
    query_results = cur.fetchall()
    print(query_results)
except Exception as e:
    print("Database connection failed due to {}".format(e))                
                



