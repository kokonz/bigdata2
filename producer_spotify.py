import time
import random
import csv
import io 
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    batch_size=32768,
    linger_ms=20,     
    compression_type='gzip',
    value_serializer=lambda v: v.encode('utf-8'),
    acks='all', 
    retries=5   
)
TOPIC = 'spotify-tracks'

def stream_data():
    try:
        with open('./dataset/spotify_data.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  
            
            total_sent = 0
            start_time = time.time()
            
            for row in reader:
                string_io = io.StringIO()
                csv_writer = csv.writer(string_io, quoting=csv.QUOTE_MINIMAL) 
                csv_writer.writerow(row)
                csv_row_safe = string_io.getvalue().strip() 
                string_io.close()
                
                producer.send(TOPIC, csv_row_safe)
                total_sent += 1
                
                if total_sent % 10000 == 0:
                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        print(f"Sent {total_sent} records | {total_sent/elapsed:.2f} records/sec")
                    else:
                        print(f"Sent {total_sent} records")
                
                time.sleep(random.uniform(0.0001, 0.001)) 
            
            producer.flush()
            print(f"Total {total_sent} records sent successfully!")

    except FileNotFoundError:
        print(f"Error: Dataset file ./dataset/spotify_data.csv not found.")
    except Exception as e:
        print(f"An error occurred in producer: {e}")
    finally:
        producer.close()


if __name__ == '__main__':
    stream_data()