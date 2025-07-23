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
    'group.id': 'consumer-fact-group1',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(topics)

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

def fact_create_topics() -> None:
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

def fact_stream_data() -> None:
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

            print('\n')
            print('1. Received message: {}'.format(msg.value().decode('utf-8')))
            print(f"2. Uploaded {object_key} to bucket '{bucket_name}'")

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
                data['user_id'], data['page_type'], data['timestamp'], date_key, data['user_id']
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
                data['user_id'], data['product_id'], data['timestamp'], date_key, data['user_id'], data['product_id'],
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
                data['user_id'], data['brand_id'], data['timestamp'], date_key, data['user_id'], data['brand_id']
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
                data['user_id'], data['order_id'], data['timestamp'], date_key, data['user_id'], data['order_id']
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
                data['user_id'], data['product_id'], data['update_type'], data['timestamp'], date_key, data['user_id'], data['product_id']
            ))
            conn.commit()

        if topic == 'auth-update':
            cur.execute("""
                INSERT INTO data_warehouse.fact_auth_update (user_id, user_sk, update_type, timestamp, date_key)
                SELECT %s, u.sk, %s, %s, %s
                FROM data_warehouse.dim_users u
                WHERE u.id = %s AND u.is_current = TRUE
            """, (
                data['user_id'], data['update_type'],  data['timestamp'], date_key, data['user_id']
            ))
            conn.commit()

        print('3. Save record to postgres successfully !!!')
        cur.close()
    except Exception as e:
        print(e)

if __name__ == '__main__':
    fact_create_topics()