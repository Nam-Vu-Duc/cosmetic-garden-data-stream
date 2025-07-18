import psycopg2
import pymongo
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from bson.objectid import ObjectId
import boto3
import json
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pyflink.table import EnvironmentSettings, TableEnvironment
from json import loads
from datetime import datetime

load_dotenv()

# kafka
topics = [
    'page-view',
    'product-view',
    'brand-view',
    'purchase',
    'cart-update',
    'auth-update',
]
admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group15',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(topics)

# flink
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# minIO
bucket_name = 'ecommerce'
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

# postgres
conn = psycopg2.connect(
    dbname="admin",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)

def streaming_data() -> None:
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]

    # Create topics
    fs = admin_client.create_topics(new_topics)

    # Wait for each topic creation result
    for topic, f in fs.items():
        try:
            f.result()
            print(f"{topic} begin to start !!!")
        except Exception as e:
            print(f"{topic} already start !!!")

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

def storing_data_lake() -> None:
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            # Decode message
            msg_str = msg.value().decode('utf-8')
            data = json.loads(msg_str)

            # Get topic, user_id, timestamp
            topic = msg.topic()
            user_id = data.get('user_id', 'unknown')
            timestamp = data.get('timestamp', 'unknown')
            timestamp = timestamp.replace(':', '-')

            # Generate object key
            object_key = f"{topic}/{user_id}_{timestamp}.json"

            # Upload to MinIO
            s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=msg_str.encode('utf-8'),
                ContentType='application/json'
            )

            print('Received message: {}'.format(msg.value().decode('utf-8')))
            print(f"Uploaded {object_key} to bucket '{bucket_name}'")

            storing_data_warehouse(object_key)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def storing_data_warehouse(object_key) -> None:
    try:
        # get data from minIO
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        data = json.loads(response['Body'].read())
        dt = datetime.strptime(data['timestamp'], "%Y-%m-%dT%H:%M:%S.%fZ")
        date_key = int(dt.strftime("%Y%m%d"))

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO data_warehouse.dim_date (date_key, year, quarter, month, day, weekday)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO NOTHING;
        """, (
            date_key,
            dt.year,
            (dt.month - 1) // 3 + 1,
            dt.month, dt.day,
            dt.strftime("%A")
        ))
        conn.commit()

        # get topic from object_key
        topic = object_key.split('/')[0]

        # insert into fact tables
        if topic == 'page-view':
            cur.execute("""
                INSERT INTO data_warehouse.fact_page_view (user_id, user_sk, page_type, timestamp, date_key)
                SELECT %s, sk, %s, %s, %s
                FROM data_warehouse.dim_users
                WHERE id = %s AND is_current = TRUE
            """, (
                data['user_id'], data['page_type'], data['timestamp'], data['user_id'], date_key
            ))
            conn.commit()

        if topic == 'product-view':
            cur.execute("""
                INSERT INTO data_warehouse.fact_product_view (user_id, user_sk, product_id, product_sk, timestamp, date_key)
                SELECT %s, u.sk, %s, p.sk, %s, %s
                FROM data_warehouse.dim_users u, data_warehouse.dim_products p
                WHERE u.id = %s AND u.is_current = TRUE
                  AND p.id = %s AND p.is_current = TRUE
            """, (
                data['user_id'], data['product_id'], data['timestamp'], data['user_id'], data['product_id'], date_key
            ))
            conn.commit()

        if topic == 'brand-view':
            cur.execute("""
                INSERT INTO data_warehouse.fact_brand_view (user_id, user_sk, brand_id, brand_sk, timestamp, date_key)
                SELECT %s, u.sk, %s, b.sk, %s, %s
                FROM data_warehouse.dim_users u, data_warehouse.dim_brands b
                WHERE u.id = %s AND u.is_current = TRUE
                  AND b.id = %s AND b.is_current = TRUE
            """, (
                data['user_id'], data['brand_id'], data['timestamp'], data['user_id'], data['brand_id'], date_key
            ))
            conn.commit()

        if topic == 'purchase':
            cur.execute("""
                INSERT INTO data_warehouse.fact_purchase (user_id, user_sk, order_id, order_sk, timestamp, date_key)
                SELECT %s, u.sk, %s, o.sk, %s, %s
                FROM data_warehouse.dim_users u, data_warehouse.dim_orders o
                WHERE u.id = %s AND u.is_current = TRUE
                  AND o.id = %s AND o.is_current = TRUE
            """, (
                data['user_id'], data['order_id'], data['timestamp'], data['user_id'], data['order_id'], date_key
            ))
            conn.commit()

        if topic == 'cart-update':
            cur.execute("""
                INSERT INTO data_warehouse.fact_cart_update (user_id, user_sk, product_id, product_sk, update_type, timestamp, date_key)
                SELECT %s, u.sk, %s, p.sk, %s, %s, %s
                FROM data_warehouse.dim_users u, data_warehouse.dim_products p
                WHERE u.id = %s AND u.is_current = TRUE
                  AND p.id = %s AND p.is_current = TRUE
            """, (
                data['user_id'], data['product_id'], data['update_type'], data['timestamp'], data['user_id'], data['product_id'], date_key
            ))
            conn.commit()

        if topic == 'auth-update':
            cur.execute("""
                INSERT INTO data_warehouse.fact_auth_update (user_id, user_sk, update_type, timestamp, date_key)
                SELECT %s, u.sk, %s, %s, %s
                FROM data_warehouse.dim_users u
                WHERE u.id = %s AND u.is_current = TRUE
            """, (
                data['user_id'], data['update_type'],  data['timestamp'], data['user_id'], date_key
            ))
            conn.commit()

        cur.close()
    except Exception as e:
        print(e)

if __name__ == '__main__':
    storing_data_lake()