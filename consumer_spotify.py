from kafka import KafkaConsumer
import csv
import io

consumer = KafkaConsumer(
    'spotify-tracks',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest', 
    group_id='spotify-group-processor', 
    fetch_max_bytes=52428800, 
    max_poll_records=10000, 
    value_deserializer=lambda v: v.decode('utf-8'),
    consumer_timeout_ms=60000 
)

BATCH_SIZE = 386588 
MAX_BATCHES = 3 

batch_number = 1
buffer = []
processed_count = 0

CSV_HEADER = [
    'id', 'artist_name', 'track_name', 'track_id', 'popularity', 'year', 'genre',
    'danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness',
    'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo',
    'duration_ms', 'time_signature'
]

def save_batch(data_buffer, batch_num):
    if not data_buffer:
        print(f"No data to save for batch {batch_num}.")
        return
    
    filename = f'batch_{batch_num}.csv'
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER) 
            writer.writerows(data_buffer)
        print(f"✅ Batch {batch_num} saved: {len(data_buffer):,} tracks to {filename}")
    except Exception as e:
        print(f"Error saving batch {batch_num}: {e}")

print("Consumer started. Waiting for messages...")
try:
    for message in consumer:
        string_io = io.StringIO(message.value)
        csv_reader = csv.reader(string_io)
        
        try:
            data_row = next(csv_reader) 
            if len(data_row) == len(CSV_HEADER): 
                buffer.append(data_row)
                processed_count += 1
            else:
                print(f"Skipping malformed row: {data_row} (expected {len(CSV_HEADER)} columns, got {len(data_row)})")
        except StopIteration:
            print(f"Skipping empty or malformed message value: {message.value}")
        finally:
            string_io.close()
        
        if len(buffer) >= BATCH_SIZE and batch_number <= MAX_BATCHES:
            save_batch(buffer[:BATCH_SIZE], batch_number)
            buffer = buffer[BATCH_SIZE:] 
            batch_number += 1
            
        if processed_count > 0 and processed_count % 10000 == 0:
            print(f"Processed {processed_count:,} records...")
            
        if batch_number > MAX_BATCHES:
            print(f"Reached maximum of {MAX_BATCHES} batches. Stopping.")
            break
            
except Exception as e:
    print(f"An error occurred in consumer: {str(e)}")
finally:
    if buffer and batch_number <= MAX_BATCHES:
        save_batch(buffer, batch_number)
    
    print(f"Consumer stopped. Total records processed: {processed_count:,}")
    consumer.close()