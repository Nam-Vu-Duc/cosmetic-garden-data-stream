import json
import os
import smtplib
import ssl
from collections import deque
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from pyflink.common import Types, Duration
from pyflink.common import WatermarkStrategy, SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

load_dotenv()

# --------- Custom TimestampAssigner for JSON input ---------
class JsonTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        try:
            event = json.loads(value)
            dt = datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            return int(dt.timestamp() * 1000)  # milliseconds
        except Exception as e:
            print(f"Error parsing timestamp: {e} for value: {value}") # Debugging print
            return 0 # Return 0 for invalid timestamps, but consider logging or filtering
                     # to avoid silent data loss or incorrect results.

# --------- LoginFloodDetector equivalent ---------
class LoginFloodDetector(KeyedProcessFunction):
    def __init__(self):
        self.login_timestamps = None

    def open(self, runtime_context: RuntimeContext):
        value_state_descriptor = ValueStateDescriptor(
            "login_timestamps_list",
            Types.LIST(Types.LONG())
        )
        self.login_timestamps = runtime_context.get_state(value_state_descriptor)

    def process_element(self, value, ctx):
        event = json.loads(value)
        user_id = event.get("user_id")
        update_type = event.get("update_type")
        current_ts = ctx.timestamp() # Event time timestamp from WatermarkStrategy

        # Only process 'login' events for this specific logic
        if update_type == "login":
            # Get current list of timestamps, or initialize if none
            timestamps = self.login_timestamps.value()
            if timestamps is None:
                timestamps = []

            # Add the current event's timestamp
            timestamps.append(current_ts)

            # Define the time window for detection (5 minutes in milliseconds)
            five_minutes_ms = 15 * 60 * 1000
            threshold_logins = 3

            # Clean up old timestamps (older than 5 minutes from the current event)
            # Convert to deque for efficient removal from the front
            timestamps_deque = deque(timestamps)
            while timestamps_deque and timestamps_deque[0] <= current_ts - five_minutes_ms:
                timestamps_deque.popleft()

            # Update the state with the cleaned list
            self.login_timestamps.update(list(timestamps_deque))

            # Check for the flood condition
            if len(timestamps_deque) > threshold_logins and \
                    (timestamps_deque[-1] - timestamps_deque[0] < five_minutes_ms):
                alert = {
                    "user_id": user_id,
                    "alert": f"Login flood detected: {len(timestamps_deque)} logins in {five_minutes_ms / 1000 / 60} minutes."
                }
                print(alert)
                send_email(alert)

# --------- Main Job ---------
def send_email(alert):
    try:
        sender = 'vuducnama6@gmail.com'
        password = os.getenv('APP_PASSWORD')
        receiver = 'namvd.dev@gmail.com'
        subject = 'Cảnh báo nghi ngờ gian lận'

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
                    <h2>Khách hàng {alert} nghi ngờ có dấu hiệu gian lận</h2>
                    <p>Hãy liên hệ và kiểm tra thông tin khách hàng trên app để xử lý kịp thời</p>
                    <p>{alert}</p>
                </body>
            </html>
        """

        em = MIMEMultipart("alternative")
        em['From'] = sender
        em['To'] = receiver
        em['Subject'] = subject
        em.attach(MIMEText(body, "html"))

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

def detect_fraud():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics('auth-update') \
        .set_group_id("flink_group1") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Watermark strategy cho xử lý thời gian sự kiện
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(JsonTimestampAssigner())

    # Tạo stream dữ liệu từ Kafka
    raw_stream = env.from_source(
        source=source,
        watermark_strategy=watermark_strategy,
        source_name="kafka_source"
    )

    # 2. Key by user_id
    keyed_stream = raw_stream.key_by(
        lambda event_str: json.loads(event_str)["user_id"],
        key_type=Types.STRING()
    )

    # Đầu ra là một MAP cho cảnh báo
    alerts = keyed_stream.process(LoginFloodDetector(), output_type=Types.MAP(Types.STRING(), Types.STRING()))

    # 4. In cảnh báo ra console
    alerts.print()

    # Thực thi job Flink
    env.execute("Login Flood Detection - PyFlink from Kafka Stream")

# Call the main function to run the Flink job
if __name__ == '__main__':
    detect_fraud()