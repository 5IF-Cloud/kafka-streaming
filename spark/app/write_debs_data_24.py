"""
Write the debs data to a Kafka topic
"""

from kafka import KafkaProducer
import pandas as pd
from time import sleep
import json

# Read the data
df = pd.read_csv('../resources/drivedata-small.csv')

# Create a Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:29092')

# Write the data to the Kafka topic

for index, row in df.iterrows():
    date = row['date']
    # Convert the date to a timestamp long
    date = pd.to_datetime(date)
    sent_data = {
        "date": int(date.timestamp()),
        "failure": row['failure'],
        "vault_id": row['vault_id']
    }
    producer.send('debs-topic', value=json.dumps(sent_data).encode('utf-8'))
    # sleep(0.4)