import json
import os
import boto3
import psycopg2
import pymongo
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

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
    'group.id': 'dim-group3',
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

# mongoDB
url      = os.getenv('MONGO_DB')
connect  = pymongo.MongoClient(url)
db_name  = os.getenv('DATABASE')
db       = connect[db_name]
users    = db['users']
products = db['products']
brands   = db['brands']
orders   = db['orders']

def dim_create_topics() -> None:
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

def dim_stream_data() -> None:
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

            print(data)

            # Get topic, user_id, timestamp
            topic = msg.topic()
            topic_type  = data['topic_type']
            emp_id      = data.get('emp_id', '')
            body        = data['body']
            timestamp = body['updatedAt'].replace(':', '-')
            date_key  = timestamp.split('T')[0]

            # Generate object key
            object_key = f"dim/{date_key}/{topic}/{emp_id}_{timestamp}.json"

            # Upload to MinIO
            s3.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=msg_str.encode('utf-8'),
                ContentType='application/json'
            )

            print('\n')
            print('1. Received message {topic}: {topic_type}')
            print(f"2. Uploaded {object_key} to minIO")

            if topic == 'create':
                insert_new_record(topic_type, body)

            if topic == 'update':
                de_active_current_record(topic_type, body)
                insert_new_record(topic_type, body)

            if topic == 'delete':
                de_active_current_record(topic_type, body)

    except Exception as e:
        print(e)

def de_active_current_record(topic_type: str, data: dict) -> None:
    try:
        cur = conn.cursor()
        if topic_type == 'customer':
            cur.execute("""
                UPDATE data_warehouse.dim_users 
                SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            conn.commit()
            return None

        if topic_type == 'product':
            cur.execute("""
                UPDATE data_warehouse.dim_products
                SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            conn.commit()
            return None

        if topic_type == 'brand':
            cur.execute("""
                UPDATE data_warehouse.dim_brands
                SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            conn.commit()
            return None

        if topic_type == 'order':
            cur.execute("""
                UPDATE data_warehouse.dim_orders
                SET end_date = CURRENT_TIMESTAMP, is_current = FALSE
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            conn.commit()
            return None

        print(f'3. Deactivated current {topic_type} record')

        return None
    except Exception as e:
        print(e)

def insert_new_record(topic_type: str, data: dict) -> None:
    try:
        cur = conn.cursor()
        if topic_type == 'customer':
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

            cur.execute("""
                SELECT sk 
                FROM data_warehouse.dim_users 
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            new_user_sk = cur.fetchone()[0]

            fact_tables = [
                "fact_page_view",
                "fact_product_view",
                "fact_brand_view",
                "fact_cart_update",
                "fact_purchase",
                "fact_auth_update"
            ]

            # Update all fact tables
            for table in fact_tables:
                cur.execute(f"""
                    UPDATE data_warehouse.{table}
                    SET user_sk = %s
                    WHERE user_id = %s
                """, (new_user_sk, str(data['_id'])))

            conn.commit()
            return None

        if topic_type == 'product':
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

            cur.execute("""
                SELECT sk 
                FROM data_warehouse.dim_products
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            new_product_sk = cur.fetchone()[0]

            fact_tables = [
                "fact_product_view",
                "fact_cart_update",
            ]

            # Update all fact tables
            for table in fact_tables:
                cur.execute(f"""
                    UPDATE data_warehouse.{table}
                    SET product_sk = %s
                    WHERE product_id = %s
                """, (new_product_sk, str(data['_id'])))

            conn.commit()
            return None

        if topic_type == 'brand':
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

            cur.execute("""
                SELECT sk 
                FROM data_warehouse.dim_brands
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            new_brand_sk = cur.fetchone()[0]

            cur.execute(f"""
                UPDATE data_warehouse.fact_brand_view
                SET brand_sk = %s
                WHERE brand_id = %s
            """, (new_brand_sk, str(data['_id'])))

            conn.commit()
            return None

        if topic_type == 'order':
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

            cur.execute("""
                SELECT sk 
                FROM data_warehouse.dim_orders
                WHERE id = %s AND is_current = TRUE
            """, (str(data['_id']),))
            new_order_sk = cur.fetchone()[0]

            cur.execute(f"""
                UPDATE data_warehouse.fact_purchase
                SET order_sk = %s
                WHERE order_id = %s
            """, (new_order_sk, str(data['_id'])))
            conn.commit()

        print(f'3. Inserted new {topic_type} record')
        return None
    except Exception as e:
        print(e)

if __name__ == '__main__':
    dim_stream_data()