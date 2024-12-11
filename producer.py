from kafka import KafkaProducer
import csv
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def stream_data(file_path, topic):
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            producer.send(topic, value=str(row).encode('utf-8'))
            time.sleep(0.1)  # Simulate real-time streaming

if __name__ == "__main__":
    topic_name = "trading-data"
    file_path = "/Users/oskari/Code/CS-E4780 Scalable Systems and Data Management/training-data/debs2022-gc-trading-day-08-11-21.csv"  # Update this path if necessary
    stream_data(file_path, topic_name)
