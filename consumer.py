from kafka import KafkaConsumer
import csv
import time

# Konfigurasi Kafka
consumer = KafkaConsumer(
    'taxi-trips',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    fetch_max_bytes=52428800, 
    max_poll_records=100000,   
    enable_auto_commit=False   
)

BATCH_SIZE = 4_249_662  
batch_number = 1
buffer = []

def save_batch(data, batch_num):
    """Simpan batch ke CSV dengan header"""
    if not data:
        return
    
    filename = f'batch_{batch_num}.csv'
    with open(filename, 'w', newline='', buffering=8192) as f:
        writer = csv.writer(f)
        writer.writerow([
            'VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
            'passenger_count', 'trip_distance', 'pickup_longitude',
            'pickup_latitude', 'RateCodeID', 'dropoff_longitude',
            'dropoff_latitude', 'payment_type', 'fare_amount'
        ])
        # Data
        writer.writerows(data)
    print(f"Batch {batch_num} saved: {len(data):,} records")

try:
    while batch_number <= 3:
        message = next(consumer)
        
        decoded = message.value.decode('utf-8').split(',')
        buffer.append(decoded)
        
        if len(buffer) >= BATCH_SIZE:
            data_to_save = buffer[:BATCH_SIZE]
            buffer = buffer[BATCH_SIZE:]
            
            save_batch(data_to_save, batch_number)
            batch_number += 1

except StopIteration:
    print("Tidak ada data lagi di topic.")
finally:
    if buffer and batch_number <= 3:
        save_batch(buffer, batch_number)
    
    # Tutup consumer
    consumer.close()
    print("Semua batch tersimpan!")