import os
from datetime import date, datetime, timedelta
import boto3
import pandas as pd
import pymongo
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
from confluent_kafka import Consumer
from river import linear_model, metrics, preprocessing, compose
import json
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, ConfusionMatrixDisplay
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

# mongoDB
url      = os.getenv('MONGO_DB')
connect  = pymongo.MongoClient(url)
db_name  = os.getenv('DATABASE')
db       = connect[db_name]
orders   = db['orders']
users    = db['users']

# kafka
topics = [
    'purchase',
    'auth-update',
]
admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'model-group5',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(topics)

def training_sale_forecast():
    try:
        order_list = orders.find()

        records = []
        for order in order_list:
            records.append({
                'date': order['createdAt'].date(),
                'revenue': order['totalNewOrderPrice']
            })

        df = pd.DataFrame(records)
        df_sales_per_day = df.groupby('date') \
            .agg(total_revenue=("revenue", "sum")) \
            .reset_index() \

        model = preprocessing.StandardScaler() | linear_model.LinearRegression()
        metric = metrics.MAE()
        chart_data = []

        # Push past data into the model
        for _, row in df_sales_per_day.iterrows():
            x = {
                'day_of_week': row['date'].weekday(),
                'month': row['date'].month,
                'day_of_month': row['date'].day,
                'week_of_year': row['date'].isocalendar()[1],
                'ordinal': row['date'].toordinal()
            }
            y = row['total_revenue']

            y_pred = model.predict_one(x) or 0

            model.learn_one(x, y)
            metric.update(y, y_pred)
            chart_data.append({
                'date': row['date'],
                'actual': y,
                'predicted': y_pred
            })

        st.title("Live Sales Forecasting")
        st.write(f"MAE: {metric.get():.2f}")
        df_chart = pd.DataFrame(chart_data)
        fig, ax = plt.subplots()
        ax.plot(df_chart['date'], df_chart['actual'], label='Actual')
        ax.plot(df_chart['date'], df_chart['predicted'], label='Predicted')
        ax.legend()
        st.pyplot(fig)

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

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

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

    clf = RandomForestClassifier()
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    print(classification_report(y_test, y_pred))

    st.title("Churn Prediction")
    fig, ax = plt.subplots()
    ConfusionMatrixDisplay.from_estimator(clf, X_test, y_test, ax=ax)
    st.pyplot(fig)
    return

def training_demand_forecast():
    order_list = orders.find()

    records = []
    for order in order_list:
        for product in order['products']:
            records.append({
                'date'       : order['createdAt'].date(),
                'id'         : product['id'],
                'name'       : product['name'],
                'quantity'   : product['quantity'],
            })

    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])

    # 2. Aggregate sales per product per day
    df_sales = df.groupby(['date', 'id', 'name']) \
        .agg(total_quantity=("quantity", "sum")) \
        .reset_index()

    # 3. Train model per product
    product_models = {}
    product_forecasts = {}

    for product_id in df_sales['id'].unique():
        product_data = df_sales[df_sales['id'] == product_id]
        product_data = product_data.sort_values('date')

        model = compose.Pipeline(
            ('scale', preprocessing.StandardScaler()),
            ('linreg', linear_model.LinearRegression())
        )
        mae = metrics.MAE()

        for _, row in product_data.iterrows():
            x = {'day': row['date'].toordinal()}
            y = row['total_quantity']
            y_pred = model.predict_one(x)
            model.learn_one(x, y)
            mae.update(y, y_pred)

        product_models[product_id] = model

        # 4. Forecast future demand
        forecast_days = 7
        last_date = product_data['date'].max()
        forecast = []
        for i in range(1, forecast_days + 1):
            future_date = last_date + timedelta(days=i)
            x_future = {'day': future_date.toordinal()}
            y_future = model.predict_one(x_future)
            forecast.append({
                'date': future_date,
                'id': product_id,
                'name': product_data['name'].iloc[0],
                'forecast_quantity': max(y_future, 0)  # Avoid negative predictions
            })
        product_forecasts[product_id] = forecast

    # 5. Combine all forecast data
    forecast_df = pd.DataFrame([f for forecasts in product_forecasts.values() for f in forecasts])

    st.write(forecast_df)

    for pid in forecast_df['id'].unique():
        chart_data = forecast_df[forecast_df['id'] == pid][['date', 'forecast_quantity']]
        st.line_chart(chart_data.set_index('date'), height=200)

    return

def sidebar():
    st.sidebar.title("Controls")

    # Refresh interval slider
    refresh_interval = st.sidebar.slider('Auto Refresh Interval (sec)', 5, 60, 10)

    # Auto refresh every X seconds
    count = st_autorefresh(interval=refresh_interval * 1000, key="datarefresh")

    # Manual refresh button
    if st.sidebar.button("Manual Refresh Now"):
        st.session_state['manual_refresh'] = datetime.now()

    # Show latest update time
    last_refresh = st.session_state.get('manual_refresh', 'Never')
    st.sidebar.write(f"Last manual refresh: {last_refresh}")

    return count

def visual_results():
    st.title('Training Model Visualization')
    training_sale_forecast()
    training_churn_prediction()
    training_demand_forecast()
    return

def main():
    sidebar()
    visual_results()

if __name__ == "__main__":
    main()