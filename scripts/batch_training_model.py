import os
import psycopg2
import boto3
import pymongo
import pandas as pd
from prophet import Prophet
from dotenv import load_dotenv
from datetime import date
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

load_dotenv()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

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
orders   = db['orders']
users    = db['users']

def training_sale_forecast():
    # forecast sale per day
    order_list = orders.find()

    records = []
    for order in order_list:
        for product in order['products']:
            records.append({
                'date'          : order['createdAt'].date(),
                'product_id'    : product['id'],
                'product_price' : product['price'],
                'quantity'      : product['quantity'],
                'revenue'       : product['totalPrice']
            })

    df = pd.DataFrame(records)
    df_sales_per_day = df.groupby('date') \
                        .agg(total_revenue=("revenue", "sum")) \
                        .reset_index() \
                        .rename(columns={'date': 'ds', 'total_revenue': 'y'})

    model = Prophet()
    model.fit(df_sales_per_day)

    future = model.make_future_dataframe(periods=30)
    forecast = model.predict(future)

    model.plot(forecast)

    # forecast sale per product
    forecast_per_product = {}

    min_date = df['date'].min()
    max_date = df['date'].max()
    full_date_range = pd.date_range(start=min_date, end=max_date, freq='D')

    for pid, group in df.groupby("product_id"):
        daily = group.groupby("date") \
            .agg(total_revenue=("revenue", "sum")) \
            .reindex(full_date_range, fill_value=0) \
            .reset_index() \
            .rename(columns={"index": "ds", "total_revenue": "y"})

        if daily['y'].sum() < 1:
            print(f"Skipping {pid}: no valid sales data.")
            continue

        model = Prophet(daily_seasonality=True)
        model.fit(daily)

        future = model.make_future_dataframe(periods=30)
        forecast = model.predict(future)
        forecast_per_product[pid] = forecast

    return

def training_churn_prediction():
    order_list = orders.find()
    user_list = users.find()

    order_list_records = []
    for order in order_list:
        if order['customerInfo']['userId'] == 'guest': continue
        order_list_records.append({
            'last_purchase_date': pd.to_datetime(order['createdAt']),
            'user_id': str(order['customerInfo']['userId']),
        })

    user_list_records = []
    for user in user_list:
        user_list_records.append({
            'last_login_date': pd.to_datetime(user['lastLogin']),
            'user_id': str(user['_id']),
        })

    order_df = pd.DataFrame(order_list_records)
    user_df = pd.DataFrame(user_list_records)

    # Latest purchase date per customer
    df_last_purchase = order_df.groupby("user_id")["last_purchase_date"] \
                        .max() \
                        .reset_index() \

    user_activity = pd.merge(user_df, df_last_purchase, on="user_id", how="left")

    user_activity['last_purchase_date'] = pd.to_datetime(user_activity['last_purchase_date'])
    user_activity['last_login_date'] = pd.to_datetime(user_activity['last_login_date'])

    user_activity['days_since_last_purchase'] = (pd.Timestamp(date.today()) - user_activity['last_purchase_date']).dt.days
    user_activity['days_since_last_login'] = (pd.Timestamp(date.today()) - user_activity['last_login_date']).dt.days

    user_activity["churn"] = (
        (user_activity["days_since_last_purchase"] > 30) |
        (user_activity["days_since_last_login"] > 7)
    ).astype(int)  # 1 = churned, 0 = active

    X = user_activity.drop(["user_id", "churn", "last_login_date", "last_purchase_date"], axis=1)
    y = user_activity["churn"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.2)

    clf = RandomForestClassifier()
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    print(classification_report(y_test, y_pred))
    return

def training_demand_forecast():
    return

if __name__ == '__main__':
    training_churn_prediction()