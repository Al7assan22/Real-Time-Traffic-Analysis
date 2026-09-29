import time
import random
import json
from datetime import datetime

import streamlit as st
from azure.eventhub import EventHubProducerClient, EventData


# Read Azure credentials from Streamlit secrets
CONNECTION_STR = st.secrets["EVENT_HUB_CONNECTION_STR"]
EVENTHUB_NAME = "traffic-data"


# Create Event Hub producer
producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENTHUB_NAME
)


streets = [
    "Ring-Road",
    "Salah-Salem-St",
    "6th-October-Bridge",
    "Nasr-City-Abbas-El-Akkad",
    "Tahrir-Square",
    "Ramses-Square",
    "Maadi-Corniche",
    "Autostrad-Road"
]


print("Sending traffic data to Azure...\n")


try:
    while True:

        vehicle_id = f"CAR-{random.randint(100, 999)}"
        speed = random.randint(10, 160)
        location = random.choice(streets)
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        traffic_data = {
            "Time": current_time,
            "Car": vehicle_id,
            "Speed": speed,
            "Location": location
        }

        event_data = EventData(json.dumps(traffic_data))

        batch = producer.create_batch()
        batch.add(event_data)

        producer.send_batch(batch)

        print(f"Sent: {traffic_data}")

        time.sleep(3)


except KeyboardInterrupt:
    print("Transmission stopped.")

finally:
    producer.close()
