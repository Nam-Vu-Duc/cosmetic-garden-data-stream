import os
import argparse
import logging
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
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

word_count_data = ["To be, or not to be,--that is the question:--",
                   "Whether 'tis nobler in the mind to suffer",
                   "The slings and arrows of outrageous fortune",
                   "Or to take arms against a sea of troubles,",
                   "And by opposing end them?--To die,--to sleep,--",
                   "No more; and by a sleep to say we end",
                   "The heartache, and the thousand natural shocks",
                   "That flesh is heir to,--'tis a consummation",
                   "Devoutly to be wish'd. To die,--to sleep;--",
                   "To sleep! perchance to dream:--ay, there's the rub;",
                   "For in that sleep of death what dreams may come,",
                   "When we have shuffled off this mortal coil,",
                   "Must give us pause: there's the respect",
                   "That makes calamity of so long life;",
                   "For who would bear the whips and scorns of time,",
                   "The oppressor's wrong, the proud man's contumely,",
                   "The pangs of despis'd love, the law's delay,",
                   "The insolence of office, and the spurns",
                   "That patient merit of the unworthy takes,",
                   "When he himself might his quietus make",
                   "With a bare bodkin? who would these fardels bear,",
                   "To grunt and sweat under a weary life,",
                   "But that the dread of something after death,--",
                   "The undiscover'd country, from whose bourn",
                   "No traveller returns,--puzzles the will,",
                   "And makes us rather bear those ills we have",
                   "Than fly to others that we know not of?",
                   "Thus conscience does make cowards of us all;",
                   "And thus the native hue of resolution",
                   "Is sicklied o'er with the pale cast of thought;",
                   "And enterprises of great pith and moment,",
                   "With this regard, their currents turn awry,",
                   "And lose the name of action.--Soft you now!",
                   "The fair Ophelia!--Nymph, in thy orisons",
                   "Be all my sins remember'd."]

def detect_fraud() -> None:
    try:
        # flink
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(1)
        env.add_jars(
            "file:///C:/Users/admin/PycharmProjects/cosmetic-garden-data-stream/jars/flink-connector-kafka-4.0.0-2.0.jar",
            "file:///C:/Users/admin/PycharmProjects/cosmetic-garden-data-stream/jars/kafka-clients-3.7.0.jar"
        )

        # Create Kafka Source
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers("broker:29092") \
            .set_topics("page-view") \
            .set_group_id("flink-consumer-group1") \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()

        # Read from Kafka as DataStream
        ds = env.from_source(
            source=kafka_source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name="Kafka Source"
        )

        # Just a basic map & print example
        processed = ds.map(lambda x: x.upper(), output_type=Types.STRING())

        # Sink - print to console
        processed.print()

        # Execute
        env.execute()

    except Exception as e:
        print(f"Error: {e}")

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

def word_count(input_path, output_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file
    env.set_parallelism(1)

    # define the source
    if input_path is not None:
        ds = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                                       input_path)
                             .process_static_file_set().build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="file_source"
        )
    else:
        print("Executing word_count example with default input data set.")
        print("Use --input to specify file input.")
        ds = env.from_collection(word_count_data)

    def split(line):
        yield from line.split()

    # compute word count
    ds = ds.flat_map(split) \
        .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()

    # submit for execution
    env.execute()

if __name__ == '__main__':
    detect_fraud()