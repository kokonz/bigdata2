import time
import csv
from kafka import KafkaProducer

# Konfigurasi Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    batch_size=32768,      
    linger_ms=20,          
    compression_type='gzip' 
)
TOPIC = 'taxi-trips'

def stream_data():
    with open('./dataset/yellow_tripdata_2015-01.csv', 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  

        total_sent = 0
        start_time = time.time()

        for row in reader:
            producer.send(TOPIC, ','.join(row).encode('utf-8'))

            total_sent += 1
            if total_sent % 10000 == 0:
                elapsed = time.time() - start_time
                print(f"Sent {total_sent} records | {total_sent/elapsed:.2f} records/sec")

            #time.sleep(0.001)

        producer.flush()
        print(f"Total {total_sent} records sent!")

if __name__ == '__main__':
    stream_data()