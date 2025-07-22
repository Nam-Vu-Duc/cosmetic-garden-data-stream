import psycopg2
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from bson.objectid import ObjectId
import boto3
import json
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from json import loads
load_dotenv()

# postgres
conn = psycopg2.connect(
    dbname="admin",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)

def batch_result_tables() -> None:
    cur = conn.cursor()
    cur.execute("""
        
    """)
    conn.commit()

    return

if __name__ == '__main__':
    batch_result_tables()