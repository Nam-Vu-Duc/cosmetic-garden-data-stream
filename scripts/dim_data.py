import json
import boto3
import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv
from datetime import datetime

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
    'group.id': 'dim-group8',
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
            print(e)

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
            print(f'1. Received message {topic}: {topic_type}')
            print(f"2. Uploaded {object_key} to minIO")

            insert_new_record(topic, topic_type, body)

    except Exception as e:
        print(e)

def insert_new_record(topic: str, topic_type: str, data: dict) -> None:
    try:
        cur = conn.cursor()
        if topic_type == 'customer':
            cur.execute("""
                INSERT INTO data_warehouse.hist_users (
                    id, email, role, name, phone, gender, address, quantity, revenue, member_code, type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                topic
            ))

            cur.execute("""
                INSERT INTO data_warehouse.dim_users (
                    id, email, role, name, phone, gender, address, quantity, revenue, member_code, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    email       = EXCLUDED.email,
                    role        = EXCLUDED.role,
                    name        = EXCLUDED.name,
                    phone       = EXCLUDED.phone,
                    gender      = EXCLUDED.gender,
                    address     = EXCLUDED.address,
                    quantity    = EXCLUDED.quantity,
                    revenue     = EXCLUDED.revenue,
                    member_code = EXCLUDED.member_code,
                    updated_at  = EXCLUDED.updated_at
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
                datetime.now()
            ))

            conn.commit()

        if topic_type == 'product':
            cur.execute("""
                INSERT INTO data_warehouse.hist_products (
                    id, categories, skincare, makeup, brand, name,
                    purchase_price, old_price, price, quantity,
                    status, rate, sale_number, rate_number, type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                data['rateNumber'],
                topic
            ))

            cur.execute("""
                INSERT INTO data_warehouse.dim_products (
                    id, categories, skincare, makeup, brand, name,
                    purchase_price, old_price, price, quantity,
                    status, rate, sale_number, rate_number, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    categories      = EXCLUDED.categories,
                    skincare        = EXCLUDED.skincare,
                    makeup          = EXCLUDED.makeup,
                    brand           = EXCLUDED.brand,
                    name            = EXCLUDED.name,
                    purchase_price  = EXCLUDED.purchase_price,
                    old_price       = EXCLUDED.old_price,
                    price           = EXCLUDED.price,
                    quantity        = EXCLUDED.quantity,
                    status          = EXCLUDED.status,
                    rate            = EXCLUDED.rate,
                    sale_number     = EXCLUDED.sale_number,
                    rate_number     = EXCLUDED.rate_number,
                    updated_at      = EXCLUDED.updated_at
            """, (
                data['id'],
                data['categories'],
                data['skincare'],
                data['makeup'],
                data['brand'],
                data['name'],
                data['purchase_price'],
                data['old_price'],
                data['price'],
                data['quantity'],
                data['status'],
                data['rate'],
                data['sale_number'],
                data['rate_number'],
                datetime.now()
            ))

            conn.commit()

        if topic_type == 'brand':
            cur.execute("""
                INSERT INTO data_warehouse.hist_brands (
                    id, name, total_product, total_revenue, type
                ) VALUES (%s, %s, %s, %s, %s)
                """, (
                str(data['_id']),
                data['name'],
                data['totalProduct'],
                data['totalRevenue'],
                topic
            ))

            cur.execute("""
                INSERT INTO data_warehouse.dim_brands (
                    id, name, total_product, total_revenue, updated_at
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name            = EXCLUDED.name,
                    total_product   = EXCLUDED.total_product,
                    total_revenue   = EXCLUDED.total_revenue,
                    updated_at      = EXCLUDED.updated_at
            """, (
                str(data['_id']),
                data['name'],
                data['totalProduct'],
                data['totalRevenue'],
                datetime.now()
            ))

            conn.commit()

        if topic_type == 'order':
            cur.execute("""
                INSERT INTO data_warehouse.hist_orders (
                    id, user_id, total_order_price, payment_method, status, type
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                str(data['_id']),
                data['customerInfo']['userId'],
                data['totalNewOrderPrice'],
                data['paymentMethod'],
                data['status'],
                topic
            ))

            cur.execute("""
                INSERT INTO data_warehouse.dim_orders (
                    id, user_id, total_order_price, payment_method, status, updated_at
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    user_id             = EXCLUDED.user_id,
                    total_order_price   = EXCLUDED.total_order_price,
                    payment_method      = EXCLUDED.payment_method,
                    status              = EXCLUDED.status,
                    updated_at          = EXCLUDED.updated_at
            """, (
                data['id'],
                data['user_id'],
                data['total_order_price'],
                data['payment_method'],
                data['status'],
                datetime.now()
            ))

            conn.commit()

        print(f'3. Inserted new {topic_type} record')
        return None
    except Exception as e:
        print(e)

if __name__ == '__main__':
    dim_create_topics()
    dim_stream_data()