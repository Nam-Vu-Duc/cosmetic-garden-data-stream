import os
import argparse
import logging
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pyflink.common import WatermarkStrategy, Encoder, Types, Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.serialization import DeserializationSchema
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.table import EnvironmentSettings, TableEnvironment, TableDescriptor
from pyflink.table.types import DataTypes
from pyflink.table.schema import Schema
from json import loads
from datetime import datetime
import ssl
import smtplib
from dotenv import load_dotenv
import time
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
load_dotenv()

def write_to_file() -> None:
    topics = [
        # 'page-view',
        # 'product-view',
        # 'brand-view',
        # 'purchase',
        # 'cart-update',
        'auth-update',
    ]
    admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'consumer-flink-2',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe(topics)

    file = open("events.json", "a")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        event = json.loads(msg.value().decode('utf-8'))
        json.dump(event, file)
        file.write('\n')
        print(f"Logged event: {event}")

        read_from_file()

        return

def read_from_file() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file
    env.set_parallelism(1)

    # Read the file as stream of strings
    source = FileSource \
        .for_record_stream_format(StreamFormat.text_line_format(),"events.json") \
        .process_static_file_set() \
        .build()

    def extract_event_timestamp(event_str):
        try:
            event = json.loads(event_str)
            dt = datetime.strptime(event["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            return int(dt.timestamp() * 1000)
        except Exception:
            return 0

    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(lambda e, ts: extract_event_timestamp(e))

    ds = env.from_source(
        source=source,
        watermark_strategy=watermark_strategy,
        source_name="file_source"
    )

    def parse_event(line):
        event = json.loads(line)
        return (event["user_id"], 1)

    parsed_stream = ds.map(parse_event, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))

    ds = ds.key_by(lambda e: e["user_id"], key_type=Types.STRING())

    result = parsed_stream \
        .key_by(lambda x: x[0]) \
        .window(SlidingEventTimeWindows.of(
        Duration.of_minutes(5),
        Duration.of_minutes(1))) \
        .sum(1) \
        .filter(lambda x: x[1] > 3)

    # --- Output (for debug purposes only) ---
    result.print()

    env.execute("Fraud Detection Job")
    return

def send_email(customer_info):
    try:
        sender = 'vuducnama6@gmail.com'
        password = os.getenv('APP_PASSWORD')
        receiver = 'namvd.dev@gmail.com'
        subject = 'Cảnh báo nghi ngờ gian lận'

        body = f"""
            <html>
                <body>
                    <h2>Khách hàng {customer_info['name']} nghi ngờ có dấu hiệu gian lận</h2>
                    <p>Hãy liên hệ và kiểm tra thông tin khách hàng trên app để xử lý kịp thời</p>
                    <p>{customer_info['name']}</p>
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

def detect_fraud() -> None:
    try:
        # define the source
        ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
            .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
        )
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        ds = env.from_collection(word_count_data)

    except KeyboardInterrupt:
        pass

if __name__ == '__main__':
    read_from_file()