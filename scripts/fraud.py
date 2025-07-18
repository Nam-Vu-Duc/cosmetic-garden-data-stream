import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pyflink.table import EnvironmentSettings, TableEnvironment
from json import loads
from datetime import datetime
import ssl
import smtplib
from dotenv import load_dotenv
import time
import mysql.connector
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
load_dotenv()

# flink
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

def detecting_fraud() -> None:
    try:
        # Define a Kafka source table
        t_env.execute_sql("""
            CREATE TABLE kafka_input (
                `user_id` STRING,
                `timestamp` STRING,
                `category` STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = 'page-view',
                'properties.bootstrap.servers' = 'broker:29092',
                'properties.group.id' = 'my-group',
                'scan.startup.mode' = 'earliest-offset',
                'format' = 'json'
            )
        """)

        # Define a print sink table
        t_env.execute_sql("""
            CREATE TABLE print_sink (
                `user_id` STRING,
                `timestamp` STRING,
                `category` STRING
            ) WITH (
                'connector' = 'print'
            )
        """)

        # Start streaming: Kafka → print
        print("Flink job started: streaming from Kafka topic `page-view`...")
        t_env.execute_sql("""
            INSERT INTO print_sink
            SELECT * FROM kafka_input
        """)

    except Exception as e:
        print(f"Error: {e}")

def send_email():
    try:
        sender = 'vuducnama6@gmail.com'
        password = os.getenv('APP_PASSWORD')
        receiver = 'namvd.dev@gmail.com'
        subject = 'Warning Fraud Detection'
        total_jobs, top_10_jobs = fetch_from_mysql()
        df = pd.DataFrame(top_10_jobs, columns=['source', 'position', 'company', 'address', 'max_salary', 'experience'])
        html = df.to_html(
            index=False,
            justify='left',
            border=1
        )

        body = f"""
            <html>
                <body>
                    <h2>Job Market Report - {time.strftime('%Y-%m-%d')}</h2>
                    <p>Total jobs: <strong>{total_jobs}</strong></p>
                    <p>Top 10 Jobs with Highest Salaries:</p>
                    {html}
                    <br>
                </body>
            </html>
        """

        em = MIMEMultipart("alternative")
        em['From'] = sender
        em['To'] = receiver
        em['Subject'] = subject
        em.attach(MIMEText(body, "html"))

        context = ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(sender, password)
            smtp.sendmail(sender, receiver, em.as_string())

    except Exception as e:
        print(e)

if __name__ == '__main__':
    detecting_fraud()