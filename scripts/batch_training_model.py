import io
import os
import smtplib
import ssl
import requests
import base64
from datetime import date
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import boto3
import pandas as pd
import psycopg2
import pymongo
from dotenv import load_dotenv
from prophet import Prophet
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split

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
    fig = model.plot(forecast)
    buf = io.BytesIO()
    fig.savefig(buf, format='png')
    buf.seek(0)
    image_bytes = buf.getvalue()
    image_base64 = base64.b64encode(image_bytes).decode("utf-8")

    message = call_gemini('Hãy phân tích ảnh này:', image_base64)
    send_email(message, io.BytesIO(image_bytes))

    # # forecast sale per product
    # forecast_per_product = {}
    #
    # min_date = df['date'].min()
    # max_date = df['date'].max()
    # full_date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    #
    # for pid, group in df.groupby("product_id"):
    #     daily = group.groupby("date") \
    #         .agg(total_revenue=("revenue", "sum")) \
    #         .reindex(full_date_range, fill_value=0) \
    #         .reset_index() \
    #         .rename(columns={"index": "ds", "total_revenue": "y"})
    #
    #     if daily['y'].sum() < 1:
    #         print(f"Skipping {pid}: no valid sales data.")
    #         continue
    #
    #     model = Prophet(daily_seasonality=True)
    #     model.fit(daily)
    #
    #     future = model.make_future_dataframe(periods=30)
    #     forecast = model.predict(future)
    #     forecast_per_product[pid] = forecast

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

def send_email(message, image):
    try:
        sender = 'vuducnama6@gmail.com'
        password = os.getenv('APP_PASSWORD')
        receiver = 'namvd.dev@gmail.com'
        subject = 'BẢN TIN DỰ BÁO KẾT QUẢ KINH DOANH TUẦN TỚI'

        # --- IMPORTANT: Check if APP_PASSWORD is set ---
        if not password:
            print("Error: APP_PASSWORD environment variable is not set.")
            print("Please set the APP_PASSWORD environment variable with your Gmail App Password.")
            print("If 2-Step Verification is enabled, you MUST use an App Password.")
            print("Refer to Google's support for 'App Passwords' or 'Less secure app access'.")
            return  # Exit the function if password is not found

        body = f"""
            <html>
                <body>
                    <h2>Kết quả dự báo kinh doanh trong tuần tới</h2>
                    <p>Dù có khả quan hay không, đây cũng chỉ là dự báo, mọi người hãy cố gắng làm việc hết sức nha</p>
                    <p>Dưới đây là phần đánh giá khách quan từ Gemini:</p>
                    <p>{message}</p>
                </body>
            </html>
        """

        em = MIMEMultipart("alternative")
        em['From'] = sender
        em['To'] = receiver
        em['Subject'] = subject
        em.attach(MIMEText(body, "html"))

        image = MIMEImage(image.read(), name="forecast.png")
        em.attach(image)

        context = ssl.create_default_context()

        print(f"Attempting to connect to SMTP server...")
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            print(f"Connected. Attempting to login as {sender}...")
            smtp.login(sender, password)
            print("Login successful. Sending email...")
            smtp.sendmail(sender, receiver, em.as_string())
            print("Email sent successfully!")

    except smtplib.SMTPAuthenticationError as auth_error:
        print(f"SMTP Authentication Error: {auth_error}")
        print("This usually means the username or password (App Password) is incorrect.")
        print("Please double-check your APP_PASSWORD environment variable and Google account settings.")

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def call_gemini(prompt, image):
    key = os.getenv('GEMINI_API_KEY')
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={key}"
    headers = {"Content-Type": "application/json"}
    body = {
        "contents": [
            {
                "parts": [
                    {"text": prompt},
                    {
                        "inlineData": {
                            "mimeType": "image/png",
                            "data": image
                        }
                    }
                ]
            }
        ]
    }

    response = requests.post(url, headers=headers, json=body)
    response.raise_for_status()
    data  = response.json()
    answer = data ["candidates"][0]["content"]["parts"][0]["text"]
    return answer

if __name__ == '__main__':
    training_sale_forecast()