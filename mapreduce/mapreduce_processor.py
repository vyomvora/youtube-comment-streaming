import multiprocessing as mp
from collections import Counter, defaultdict
import pandas as pd
import time
import numpy as np
from functools import partial
import sys
import os
import boto3
import json
from datetime import datetime
import io

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from data_processor import DataProcessor

class MapReduceProcessor:
    def __init__(self, num_processes=None, s3_output_bucket="youtube-comment-output"):
        self.num_processes = num_processes or mp.cpu_count()
        self.s3_bucket = s3_output_bucket
    
    def map_word_count(self, text_chunk):
        """Map function: Extract and count words from text chunk"""
        word_count = Counter()
        
        for text in text_chunk:
            if pd.notna(text) and text.strip():
                words = str(text).lower().split()
                # Filter out very short words
                words = [word for word in words if len(word) > 2]
                word_count.update(words)
        
        return word_count
    
    def map_sentiment_count(self, data_chunk):
        """Map function: Count sentiment distribution"""
        sentiment_count = Counter()
        
        for sentiment in data_chunk:
            if pd.notna(sentiment):
                sentiment_count[sentiment] += 1
        
        return sentiment_count
    
    def map_word_sentiment(self, data_chunk):
        """Map function: Analyze words by sentiment"""
        word_sentiment = defaultdict(lambda: {'positive': 0, 'negative': 0, 'neutral': 0})
        
        for comment, sentiment in data_chunk:
            if pd.notna(comment) and pd.notna(sentiment):
                words = str(comment).lower().split()
                words = [word for word in words if len(word) > 2]
                
                for word in words:
                    word_sentiment[word][sentiment] += 1
        
        return dict(word_sentiment)
    
    def reduce_counters(self, counter_list):
        """Reduce function: Combine multiple counters"""
        total_count = Counter()
        for counter in counter_list:
            total_count.update(counter)
        return total_count
    
    def reduce_word_sentiment(self, word_sentiment_list):
        """Reduce function: Combine word-sentiment mappings"""
        combined = defaultdict(lambda: {'positive': 0, 'negative': 0, 'neutral': 0})
        
        for word_dict in word_sentiment_list:
            for word, sentiments in word_dict.items():
                for sentiment, count in sentiments.items():
                    combined[word][sentiment] += count
        
        return dict(combined)
    
    def save_to_s3(self, data, filename, data_type="json"):
        """Save data to S3 bucket"""
        try:
            # Create S3 client only when needed
            s3_client = boto3.client('s3')
            s3_key = f"mapreduce-results/{filename}"
            
            if data_type == "json":
                # Convert data to JSON
                json_data = json.dumps(data, indent=2, default=str)
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=json_data,
                    ContentType='application/json'
                )
            
            elif data_type == "csv":
                # Convert DataFrame to CSV
                csv_buffer = io.StringIO()
                data.to_csv(csv_buffer, index=False)
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=csv_buffer.getvalue(),
                    ContentType='text/csv'
                )
            
            print(f"Saved to S3 {self.s3_bucket}/{s3_key}")
            return f"s3://{self.s3_bucket}/{s3_key}"
            
        except Exception as e:
            print(f"Error saving to S3: {e}")
            return None
    
    def parallel_word_count(self, comments):
        """Execute parallel word counting using MapReduce"""
        start_time = time.time()
        
        # Split data into chunks
        chunk_size = max(1, len(comments) // self.num_processes)
        chunks = [comments[i:i+chunk_size] for i in range(0, len(comments), chunk_size)]
        
        print(f"Processing {len(comments)} comments in {len(chunks)} chunks...")
        
        # Map phase - parallel processing
        with mp.Pool(self.num_processes) as pool:
            map_results = pool.map(self.map_word_count, chunks)
        
        # Reduce phase - combine results
        final_count = self.reduce_counters(map_results)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"Parallel word count completed in {processing_time:.2f} seconds")
        return final_count, processing_time
    
    def parallel_sentiment_analysis(self, sentiments):
        """Execute parallel sentiment counting"""
        start_time = time.time()
        
        # Split data into chunks
        chunk_size = max(1, len(sentiments) // self.num_processes)
        chunks = [sentiments[i:i+chunk_size] for i in range(0, len(sentiments), chunk_size)]
        
        print(f"Processing sentiment for {len(sentiments)} comments...")
        
        # Map phase
        with mp.Pool(self.num_processes) as pool:
            map_results = pool.map(self.map_sentiment_count, chunks)
        
        # Reduce phase
        final_count = self.reduce_counters(map_results)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"Parallel sentiment analysis completed in {processing_time:.2f} seconds")
        return final_count, processing_time
    
    def parallel_word_sentiment_analysis(self, comments, sentiments):
        """Analyze words by their associated sentiments"""
        start_time = time.time()
        
        # Combine comments and sentiments
        data = list(zip(comments, sentiments))
        
        # Split into chunks
        chunk_size = max(1, len(data) // self.num_processes)
        chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]
        
        print(f"Processing word-sentiment analysis for {len(data)} comments...")
        
        # Map phase
        with mp.Pool(self.num_processes) as pool:
            map_results = pool.map(self.map_word_sentiment, chunks)
        
        # Reduce phase
        final_result = self.reduce_word_sentiment(map_results)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"Word-sentiment analysis completed in {processing_time:.2f} seconds")
        return final_result, processing_time
    
    def sequential_word_count(self, comments):
        """Sequential word counting for comparison"""
        start_time = time.time()
        
        word_count = Counter()
        for text in comments:
            if pd.notna(text) and text.strip():
                words = str(text).lower().split()
                words = [word for word in words if len(word) > 2]
                word_count.update(words)
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"Sequential word count completed in {processing_time:.2f} seconds")
        return word_count, processing_time
    
    def benchmark_performance(self, comments, sentiments=None):
        """Compare sequential vs parallel performance and save to S3"""
        print("\n" + "="*50)
        print("PERFORMANCE BENCHMARK")
        print("="*50)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results = {
            'timestamp': timestamp,
            'total_comments': len(comments),
            'num_processes': self.num_processes
        }
        
        # Sequential processing
        print("\n1. Sequential Processing:")
        seq_word_count, seq_time = self.sequential_word_count(comments)
        results['sequential'] = {
            'time': seq_time,
            'word_count': len(seq_word_count),
            'top_words': seq_word_count.most_common(10)
        }
        
        # Parallel processing
        print("\n2. Parallel Processing:")
        par_word_count, par_time = self.parallel_word_count(comments)
        results['parallel'] = {
            'time': par_time,
            'word_count': len(par_word_count),
            'top_words': par_word_count.most_common(10)
        }
        
        # Calculate speedup
        speedup = seq_time / par_time if par_time > 0 else 0
        efficiency = speedup / self.num_processes
        
        print(f"\n PERFORMANCE RESULTS:")
        print(f"Sequential time: {seq_time:.2f} seconds")
        print(f"Parallel time: {par_time:.2f} seconds")
        print(f"Speedup: {speedup:.2f}x")
        print(f"Efficiency: {efficiency:.2f}")
        print(f"Processes used: {self.num_processes}")
        
        results['metrics'] = {
            'speedup': speedup,
            'efficiency': efficiency,
            'processes': self.num_processes
        }
        
        # Sentiment analysis if provided
        if sentiments is not None:
            print("\n3. Sentiment Analysis:")
            sentiment_count, sentiment_time = self.parallel_sentiment_analysis(sentiments)
            results['sentiment'] = {
                'time': sentiment_time,
                'distribution': dict(sentiment_count)
            }
            print(f"Sentiment distribution: {dict(sentiment_count)}")
            
            # Word-sentiment analysis
            print("\n4. Word-Sentiment Analysis:")
            word_sentiment, ws_time = self.parallel_word_sentiment_analysis(comments, sentiments)
            results['word_sentiment'] = {
                'time': ws_time,
                'analysis': word_sentiment
            }
        
        # Save all results to S3
        self.save_results_to_s3(results, seq_word_count, par_word_count, timestamp)
        
        return results
    
    def save_results_to_s3(self, results, seq_word_count, par_word_count, timestamp):
        """Save all MapReduce results to S3"""
        print(f"\n Saving results to S3...")
        
        # 1. Save benchmark summary
        summary_filename = f"benchmark_summary_{timestamp}.json"
        self.save_to_s3(results, summary_filename, "json")
        
        # 2. Save detailed word counts
        word_count_data = {
            'sequential_word_count': dict(seq_word_count.most_common(100)),
            'parallel_word_count': dict(par_word_count.most_common(100)),
            'timestamp': timestamp
        }
        word_filename = f"word_counts_{timestamp}.json"
        self.save_to_s3(word_count_data, word_filename, "json")
        
        # 3. Save performance metrics as CSV
        performance_df = pd.DataFrame({
            'method': ['sequential', 'parallel'],
            'time_seconds': [results['sequential']['time'], results['parallel']['time']],
            'unique_words': [results['sequential']['word_count'], results['parallel']['word_count']],
            'speedup': [1.0, results['metrics']['speedup']],
            'efficiency': [1.0, results['metrics']['efficiency']],
            'timestamp': [timestamp, timestamp]
        })
        perf_filename = f"performance_metrics_{timestamp}.csv"
        self.save_to_s3(performance_df, perf_filename, "csv")
        
        # 4. Save top words comparison
        top_words_df = pd.DataFrame({
            'word': [word for word, count in results['parallel']['top_words']],
            'count': [count for word, count in results['parallel']['top_words']],
            'rank': range(1, len(results['parallel']['top_words']) + 1),
            'timestamp': timestamp
        })
        words_filename = f"top_words_{timestamp}.csv"
        self.save_to_s3(top_words_df, words_filename, "csv")
        
        # 5. Save sentiment analysis if available
        if 'sentiment' in results:
            sentiment_df = pd.DataFrame({
                'sentiment': list(results['sentiment']['distribution'].keys()),
                'count': list(results['sentiment']['distribution'].values()),
                'timestamp': timestamp
            })
            sentiment_filename = f"sentiment_analysis_{timestamp}.csv"
            self.save_to_s3(sentiment_df, sentiment_filename, "csv")
        
        print(f"All results saved to S3 bucket: {self.s3_bucket}/mapreduce-results/")

def main():
    """Test the MapReduce processor with S3 output"""
    print("Testing MapReduce Processor with S3 Output")
    print("="*50)
    s3_input_bucket = "youtube-comments-input" 
    s3_input_file = "youtubecommentscleaned.csv"
    
    # Load and preprocess data
    processor = DataProcessor(s3_input_bucket, s3_input_file)
    
    # Use first 5000 comments for testing (you can increase this)
    processor.df = processor.df.head(5000)
    processed_df = processor.preprocess_data()
    
    # Initialize MapReduce with S3 output
    mapreduce = MapReduceProcessor(s3_output_bucket="youtube-comments-output")
    
    # Run benchmark
    results = mapreduce.benchmark_performance(
        comments=processed_df['cleaned_comment'].tolist(),
        sentiments=processed_df['sentiment_vader'].tolist()
    )
    
    # Display top words
    print(f"\n TOP 10 WORDS:")
    for word, count in results['parallel']['top_words']:
        print(f"{word}: {count}")
    
    print(f"\n MapReduce processing completed!")
    print(f"Check S3 bucket 'youtube-comment-output/mapreduce-results/' for all results")

if __name__ == "__main__":
    main()