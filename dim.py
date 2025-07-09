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
from json import loads
load_dotenv()

# kafka
topics = [
    'create',
    'update',
    'delete',
]
admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group1',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(topics)

# postgres
conn = psycopg2.connect(
    dbname="admin",
    user="admin",
    password="admin",
    host="localhost",
    port="5432"
)

# mongoDB
url = os.getenv('MONGO_DB')
connect = pymongo.MongoClient(url)
db = connect['bunnyStore_database']
users = db['users']
products = db['products']
brands = db['brands']
orders = db['orders']

def streaming_data():
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

def storing_data_warehouse():
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

            if topic == 'create':
                cur = conn.cursor()
                if type == 'users':
                    cur.execute("""
                        INSERT INTO data_warehouse.dim_users (
                            id, email, role, name, phone, gender, address, quantity, revenue, member_code
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                        str(data['_id']),
                        data['email'],
                        data['role'],
                        data['name'],
                        data['phone'],
                        'F' if data['gender'] == 'female' else 'M',
                        data['address'],
                        data['quantity'],
                        data['revenue'],
                        data['memberCode'],
                    ))
                    conn.commit()

                if type == 'product':
                    cur.execute("""
                        INSERT INTO data_warehouse.dim_products (
                            id, categories, skincare, makeup, brand, name,
                            purchase_price, old_price, price, quantity,
                            status, rate, sale_number, rate_number
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                        str(data['_id']),
                        data['categories'],
                        data['skincare'],
                        data['makeup'],
                        data['brand'],
                        data['name'],
                        data['purchasePrice'],
                        data['oldPrice'],
                        data['price'],
                        data['quantity'],
                        data['status'],
                        data['rate'],
                        data['saleNumber'],
                        data['rateNumber']
                    ))
                    conn.commit()

                if type == 'brand':
                    cur.execute("""
                        INSERT INTO data_warehouse.dim_brands (
                            id, name, total_product, total_revenue
                        ) VALUES (%s, %s, %s, %s)
                        """, (
                        str(data['_id']),
                        data['name'],
                        data['totalProduct'],
                        data['totalRevenue'],
                    ))
                    conn.commit()

                if type == 'order':
                    cur.execute("""
                        INSERT INTO data_warehouse.dim_orders (
                            id, user_id, total_order_price, payment_method, status
                            ) VALUES (%s, %s, %s, %s, %s)
                        """, (
                        str(data['_id']),
                        data['customerInfo']['userId'],
                        data['totalNewOrderPrice'],
                        data['paymentMethod'],
                        data['status'],
                    ))
                    conn.commit()

            if topic == 'update':
                continue

            if topic == 'delete':
                continue

    except Exception as e:
        print(e)

if __name__ == '__main__':
    storing_data_warehouse()