import pandas as pd
import boto3
import re
import nltk
from textblob import TextBlob
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
from datetime import datetime, timedelta
import random

class DataProcessor:
    def __init__(self, s3_input_bucket, s3_input_file):
        self.analyzer = SentimentIntensityAnalyzer()
        self.s3_client = boto3.client('s3')
        response = self.s3_client.get_object(Bucket=s3_input_bucket, Key=s3_input_file)
        self.df = pd.read_csv(response['Body'])
        print("--------------------------------------")
        print("self df", self.df)
        self.df = self.df.rename(columns={'comments': 'Comment'})

    def clean_text(self, text):
        """Clean and normalize text"""
        if pd.isna(text):
            return ""
        
        # Convert to string and lowercase
        text = str(text).lower()
        
        # Remove URLs, mentions, hashtags
        text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
        text = re.sub(r'@\w+|#\w+', '', text)
        
        # Remove special characters but keep spaces
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        
        # Remove extra whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def get_sentiment_vader(self, text):
        """Get sentiment using VADER"""
        if not text:
            return 'neutral'
        
        score = self.analyzer.polarity_scores(text)
        
        if score['compound'] >= 0.05:
            return 'positive'
        elif score['compound'] <= -0.05:
            return 'negative'
        else:
            return 'neutral'
    
    def get_sentiment_textblob(self, text):
        """Get sentiment using TextBlob"""
        if not text:
            return 'neutral'
        
        polarity = TextBlob(text).sentiment.polarity
        
        if polarity > 0.1:
            return 'positive'
        elif polarity < -0.1:
            return 'negative'
        else:
            return 'neutral'
    
    def preprocess_data(self):
        """Complete data preprocessing pipeline"""
        print("Starting data preprocessing...")
        
        # Clean comments
        self.df['cleaned_comment'] = self.df['Comment'].apply(self.clean_text)
        
        # Add sentiment analysis
        print("Adding sentiment analysis...")
        self.df['sentiment_vader'] = self.df['cleaned_comment'].apply(self.get_sentiment_vader)
        self.df['sentiment_textblob'] = self.df['cleaned_comment'].apply(self.get_sentiment_textblob)
        
        # Add timestamps for streaming simulation
        print("Adding timestamps...")
        self.df['timestamp'] = self.generate_timestamps()
        
        # Add word count
        self.df['word_count'] = self.df['cleaned_comment'].apply(lambda x: len(x.split()))
        
        # Remove empty comments
        self.df = self.df[self.df['cleaned_comment'].str.len() > 0]
        
        print(f"Preprocessing complete. {len(self.df)} valid comments remaining.")
        return self.df
    
    def generate_timestamps(self):
        """Generate realistic timestamps for streaming simulation"""
        start_time = datetime.now() - timedelta(hours=24)
        timestamps = []
        
        for i in range(len(self.df)):
            # Add some randomnesc
            random_minutes = random.randint(0, 1440)
            timestamp = start_time + timedelta(minutes=random_minutes)
            timestamps.append(timestamp)
        
        return sorted(timestamps)
    
    def get_sample_data(self, n=1000):
        """Get sample data for testing"""
        return self.df.sample(n=min(n, len(self.df)))
    
    def save_processed_data(self, output_path):
        """Save processed data"""
        self.df.to_csv(output_path, index=False)
        print(f"Processed data saved to {output_path}")
    
    def get_basic_stats(self):
        """Get basic statistics"""
        if hasattr(self, 'df'):
            stats = {
                'total_comments': len(self.df),
                'avg_word_count': self.df['word_count'].mean(),
                'sentiment_distribution': self.df['sentiment_vader'].value_counts().to_dict(),
                'date_range': f"{self.df['timestamp'].min()} to {self.df['timestamp'].max()}"
            }
            return stats
        return None