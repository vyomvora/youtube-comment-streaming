#!/usr/bin/env python3

import boto3
import pandas as pd
from kafka import KafkaProducer
import json
import time
import sys
import os
from datetime import datetime, timedelta

class S3KafkaIngestion:
    def __init__(self, kafka_host, kafka_port=9092, topic_name='youtube-comments'):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.topic_name = topic_name
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[f'{kafka_host}:{kafka_port}'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None
        )
        
        print(f"Initialized ingestion: S3 → Kafka ({kafka_host}:{kafka_port})")
        print(f"Target topic: {topic_name}")
    
    def read_from_s3(self, bucket_name, file_key):
        try:
            print(f"Reading from S3: s3://{bucket_name}/{file_key}")
            
            # Download file from S3
            response = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
            
            # Read CSV data
            df = pd.read_csv(response['Body'])
            print(f"Loaded {len(df)} records from S3")
            
            return df
            
        except Exception as e:
            print(f"Error reading from S3: {e}")
            return None
    
    def preprocess_comment(self, comment_row, base_time, index):
        # Calculate incremental timestamp (30 seconds apart)
        comment_timestamp = base_time + timedelta(seconds=index * 30)
        
        return {
            'comment_id': str(comment_row.name),
            'comment_text': str(comment_row['Comment']),
            'timestamp': comment_timestamp.isoformat(),
            'processed_at': datetime.now().isoformat()
        }
    
    def stream_to_kafka(self, df, batch_size=10, delay_seconds=1):
        total_records = len(df)
        sent_count = 0
        base_time = datetime.now()

        
        print(f"Starting stream to Kafka...")
        print(f"Total records: {total_records}")
        print(f"Batch size: {batch_size}")
        print(f"Delay: {delay_seconds}s between batches")
        print("-" * 50)
        
        try:
            for i in range(0, total_records, batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Preprocess comment
                    comment_data = self.preprocess_comment(row, base_time, sent_count)
                    
                    # Send to Kafka
                    self.producer.send(
                        topic=self.topic_name,
                        key=comment_data['comment_id'],
                        value=comment_data
                    )
                    
                    sent_count += 1
                
                # Flush and wait
                self.producer.flush()
                
                print(f"Sent batch {i//batch_size + 1}: {sent_count}/{total_records} records")
                
                # Simulate real-time streaming
                if i + batch_size < total_records:
                    time.sleep(delay_seconds)
            
            print(f"Streaming completed! Sent {sent_count} records to Kafka")
            
        except Exception as e:
            print(f"Error during streaming: {e}")
        
        finally:
            self.producer.close()
    
    def run_ingestion(self, bucket_name, file_key, batch_size=10, delay_seconds=1):
        print("Starting S3 to Kafka Ingestion Pipeline")        
        df = self.read_from_s3(bucket_name, file_key)
        if df is None:
            print("Failed to read data from S3. Stopping.")
            return False
        self.stream_to_kafka(df, batch_size, delay_seconds)
        
        print("\n Ingestion pipeline completed!")
        return True

def main():
    
    KAFKA_HOST = "35.173.231.218"
    KAFKA_PORT = 9092
    KAFKA_TOPIC = "youtube-comments"
    
    # S3 bucket
    S3_BUCKET = "youtube-comments-input" 
    S3_FILE_KEY = "youtubecommentscleaned.csv"
    
    # batch settings
    BATCH_SIZE = 100
    DELAY_SECONDS = 2
    
    
    print("YouTube Comment Data Ingestion")
    print(f"Source: s3://{S3_BUCKET}/{S3_FILE_KEY}")
    print(f"Target: Kafka @ {KAFKA_HOST}:{KAFKA_PORT}")
    print(f"Topic: {KAFKA_TOPIC}")
    
    
    try:
        ingestion = S3KafkaIngestion(
            kafka_host=KAFKA_HOST,
            kafka_port=KAFKA_PORT,
            topic_name=KAFKA_TOPIC
        )
        
        # Run  pipeline
        success = ingestion.run_ingestion(
            bucket_name=S3_BUCKET,
            file_key=S3_FILE_KEY,
            batch_size=BATCH_SIZE,
            delay_seconds=DELAY_SECONDS
        )
        
        if success:
            print("\n Data ingestion completed successfully!")
        else:
            print("\n Data ingestion failed!")
            
    except KeyboardInterrupt:
        print("\n  Ingestion stopped by user")
    except Exception as e:
        print(f"\n Unexpected error: {e}")

if __name__ == "__main__":
    main()