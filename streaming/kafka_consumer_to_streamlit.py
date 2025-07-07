import streamlit as st
import pandas as pd
from kafka import KafkaConsumer
import json
import threading
import time
from datetime import datetime, timedelta
from textblob import TextBlob
import plotly.express as px
import plotly.graph_objects as go
from collections import deque, Counter
import queue
import re
from typing import List, Dict, Tuple
import psutil

class SlidingWindowAnalyzer:
    def __init__(self):
        self.all_messages = deque(maxlen=10000)
        
    def add_message(self, message_data: Dict):
        """Add message with current timestamp"""
        message_data['actual_timestamp'] = datetime.now()
        self.all_messages.append(message_data)
    
    def get_messages_in_window(self, minutes: int) -> List[Dict]:
        """Get all messages within the last N minutes"""
        cutoff_time = datetime.now() - timedelta(minutes=minutes)
        return [msg for msg in self.all_messages 
                if msg.get('actual_timestamp', datetime.now()) > cutoff_time]
    
    def get_top_words(self, minutes: int, top_n: int = 5) -> List[Tuple[str, int]]:
        """Get top N words from messages in the last N minutes"""
        messages_in_window = self.get_messages_in_window(minutes)
        
        if not messages_in_window:
            return []
        
        # Combine all text from messages in window
        all_text = ' '.join([msg.get('full_text', '') for msg in messages_in_window])
        
        # Clean and tokenize text
        words = self._extract_words(all_text)
        
        # Count words and return top N
        word_counts = Counter(words)
        return word_counts.most_common(top_n)
    
    def get_sentiment_stats(self, minutes: int) -> Dict:
        """Get sentiment statistics for messages in window"""
        messages_in_window = self.get_messages_in_window(minutes)
        
        if not messages_in_window:
            return {'positive': 0, 'negative': 0, 'neutral': 0, 'total': 0}
        
        sentiments = [msg.get('sentiment', 'Neutral') for msg in messages_in_window]
        sentiment_counts = Counter(sentiments)
        
        return {
            'positive': sentiment_counts.get('Positive', 0),
            'negative': sentiment_counts.get('Negative', 0),
            'neutral': sentiment_counts.get('Neutral', 0),
            'total': len(messages_in_window)
        }
    
    def get_message_rate(self, minutes: int) -> float:
        """Get messages per minute rate for the window"""
        messages_in_window = self.get_messages_in_window(minutes)
        if not messages_in_window or minutes == 0:
            return 0.0
        return len(messages_in_window) / minutes
    
    def _extract_words(self, text: str) -> List[str]:
        """Extract meaningful words from text"""
        # Convert to lowercase and remove special characters
        text = re.sub(r'[^\w\s]', ' ', text.lower())
        
        # Split into words
        words = text.split()
        
        # common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 
            'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'have', 'has', 'had',
            'do', 'does', 'did', 'will', 'would', 'could', 'should', 'this', 'that',
            'these', 'those', 'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves',
            'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself',
            'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their',
            'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'whose', 'why', 'how',
            'where', 'when', 'so', 'than', 'too', 'very', 'can', 'just', 'now', 'also'
        }
        
        # Filter stop words and numbers
        filtered_words = [
            word for word in words 
            if len(word) > 2 and word not in stop_words and not word.isdigit()
        ]
        
        return filtered_words

class KafkaStreamProcessor:
    def __init__(self, kafka_host="3.89.117.169", kafka_port=9092, topic="youtube-comments"):
        self.kafka_host = kafka_host
        self.kafka_port = kafka_port
        self.topic = topic
        self.consumer = None
        self.is_running = False
        
        self.message_queue = queue.Queue(maxsize=1000)
        
        # Data storage (keep last 100 messages for display)
        self.comments_data = deque(maxlen=100)
        self.sentiment_history = deque(maxlen=50)
        
        # Sliding window analyzer
        self.window_analyzer = SlidingWindowAnalyzer()
        
    def get_sentiment(self, text):
        """Simple sentiment analysis using TextBlob"""
        try:
            blob = TextBlob(str(text))
            polarity = blob.sentiment.polarity
            if polarity > 0.1:
                return "Positive", polarity
            elif polarity < -0.1:
                return "Negative", polarity
            else:
                return "Neutral", polarity
        except:
            return "Neutral", 0.0
    
    def process_message(self, message_data):
        """Process individual Kafka message"""
        try:
            comment_text = message_data.get('comment_text', '')
            timestamp = message_data.get('timestamp', datetime.now().isoformat())
            
            # Get sentiment
            sentiment, polarity = self.get_sentiment(comment_text)
            
            # Count words
            word_count = len(str(comment_text).split())
            
            # Create processed record
            processed_record = {
                'comment_id': message_data.get('comment_id', ''),
                'comment_text': comment_text[:100] + '...' if len(comment_text) > 100 else comment_text,
                'full_text': comment_text,
                'timestamp': timestamp,
                'sentiment': sentiment,
                'polarity': polarity,
                'word_count': word_count,
                'processed_time': datetime.now().strftime('%H:%M:%S')
            }
            
            return processed_record
            
        except Exception as e:
            st.error(f"Error processing message: {e}")
            return None
    
    def kafka_consumer_thread(self):
        """Background thread to consume Kafka messages"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[f'{self.kafka_host}:{self.kafka_port}'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000,
                auto_offset_reset='latest',
                group_id='streamlit-dashboard',
                enable_auto_commit=True
            )
            
            print(f"Connected to Kafka: {self.kafka_host}:{self.kafka_port}")
            print(f"Consuming from topic: {self.topic}")
            
            partitions = self.consumer.partitions_for_topic(self.topic)
            print(f"Topic partitions: {partitions}")
            
            message_count = 0
            while self.is_running:
                try:
                    message_batch = self.consumer.poll(timeout_ms=1000)
                    
                    if message_batch:
                        print(f"Received batch with {sum(len(messages) for messages in message_batch.values())} messages")
                    
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            if not self.is_running:
                                break
                            
                            message_count += 1
                            print(f"Processing message #{message_count}: {str(message.value)[:100]}...")
                                
                            # Process message
                            processed_record = self.process_message(message.value)
                            
                            if processed_record:
                                # Add to sliding window analyzer
                                self.window_analyzer.add_message(processed_record)
                                
                                try:
                                    self.message_queue.put_nowait(processed_record)
                                    print(f"Added message to queue: {processed_record['comment_id']}")
                                except queue.Full:
                                    try:
                                        self.message_queue.get_nowait()
                                        self.message_queue.put_nowait(processed_record)
                                        print("Queue full, replaced oldest message")
                                    except queue.Empty:
                                        pass
                            else:
                                print("Failed to process message")
                    
                    if message_count % 10 == 0 and message_count > 0:
                        print(f" Processed {message_count} messages so far...")
                                        
                except Exception as e:
                    if self.is_running:
                        print(f"Consumer error: {e}")
                    time.sleep(1)
                    
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            if hasattr(st.session_state, 'kafka_error'):
                st.session_state.kafka_error = str(e)
        finally:
            if self.consumer:
                self.consumer.close()
                print("Kafka consumer closed")
    
    def start_consuming(self):
        """Start the Kafka consumer in background thread"""
        if not self.is_running:
            self.is_running = True
            consumer_thread = threading.Thread(target=self.kafka_consumer_thread, daemon=True)
            consumer_thread.start()
            return True
        return False
    
    def stop_consuming(self):
        """Stop the Kafka consumer"""
        self.is_running = False
        if self.consumer:
            self.consumer.close()
    
    def get_new_messages(self):
        """Get all new messages from queue"""
        new_messages = []
        while True:
            try:
                message = self.message_queue.get_nowait()
                new_messages.append(message)
                self.comments_data.append(message)
                self.sentiment_history.append({
                    'time': message['processed_time'],
                    'sentiment': message['sentiment'],
                    'polarity': message['polarity']
                })
            except queue.Empty:
                break
        return new_messages

def render_sliding_window_metrics(processor):
    st.subheader("Last 5 Minutes Analysis")
    
    # Get window statistics (fixed 5 minutes)
    sentiment_stats = processor.window_analyzer.get_sentiment_stats(5)
    message_rate = processor.window_analyzer.get_message_rate(5)
    top_words = processor.window_analyzer.get_top_words(5, 5)
    
    # Metrics row
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        st.metric("Messages", sentiment_stats['total'])
    
    with col2:
        st.metric(" Rate (msg/min)", f"{message_rate:.1f}")
    
    with col3:
        if sentiment_stats['total'] > 0:
            pos_pct = (sentiment_stats['positive'] / sentiment_stats['total']) * 100
            st.metric("Positive %", f"{pos_pct:.1f}%")
        else:
            st.metric("Positive %", "0.0%")
    
    with col4:
        st.metric("Window Size", "5m")
    
    with col5:
        st.metric("CPU Usage", f"{psutil.cpu_percent()}%")

    with col6:
        st.metric("Memory Usage", f"{psutil.virtual_memory().percent}%")
    
    # Top words section
    if top_words:
        st.markdown(" Top 5 Words (Last 5 Minutes)")
        
        # Create two columns for better layout
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Create a horizontal bar chart
            words, counts = zip(*top_words)
            fig_words = px.bar(
                x=counts,
                y=words,
                orientation='h',
                title="Most Frequent Words (Last 5 minutes)",
                labels={'x': 'Count', 'y': 'Words'}
            )
            fig_words.update_layout(height=300, yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_words, use_container_width=True)
        
        with col2:
            # Show top words as a list
            st.markdown("**Word Counts:**")
            for word, count in top_words:
                st.write(f"• **{word}**: {count}")
    else:
        st.info("No messages found in the last 5 minutes")

def main():
    st.set_page_config(
        page_title="YouTube Comments Real-time Dashboard with Sliding Windows",
        layout="wide"
    )
    
    st.title("YouTube Comments Real-time Analytics with Sliding Windows")
    st.markdown("---")
    
    # Initialize session state
    if 'processor' not in st.session_state:
        st.session_state.processor = KafkaStreamProcessor()
        st.session_state.total_messages = 0
        st.session_state.is_consuming = False
        st.session_state.kafka_error = None
        st.session_state.debug_info = []
    
    # Control panel
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        if st.session_state.kafka_error:
            st.error(f"Kafka Error: {st.session_state.kafka_error}")
        
        queue_size = st.session_state.processor.message_queue.qsize()
        data_size = len(st.session_state.processor.comments_data)
        window_size = len(st.session_state.processor.window_analyzer.all_messages)
        st.write(f"**Queue Size:** {queue_size}")
        st.write(f"**Display Buffer:** {data_size}")
        st.write(f"**Window Buffer:** {window_size}")
    
    with col2:
        if st.button("Start Kafka", disabled=st.session_state.is_consuming):
            st.session_state.kafka_error = None
            if st.session_state.processor.start_consuming():
                st.session_state.is_consuming = True
                st.success("Started consuming from Kafka!")
                st.rerun()
    
    with col3:
        if st.button("Stop Kafka", disabled=not st.session_state.is_consuming):
            st.session_state.processor.stop_consuming()
            st.session_state.is_consuming = False
            st.info("Stopped consuming from Kafka")
            st.rerun()
    
    # 5-Minute Sliding Window Analysis
    st.markdown("---")
    if st.session_state.processor.window_analyzer.all_messages:
        render_sliding_window_metrics(st.session_state.processor)
    
    # Auto-refresh
    if st.session_state.is_consuming:
        new_messages = st.session_state.processor.get_new_messages()
        if new_messages:
            st.session_state.total_messages += len(new_messages)
            st.success(f"Received {len(new_messages)} new messages!")
        
        time.sleep(2)
        st.rerun()
    
    st.markdown("---")
    
    # Dashboard metrics (existing functionality)
    if st.session_state.processor.comments_data:
        st.subheader("Overall Statistics")
        
        # Metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        comments_list = list(st.session_state.processor.comments_data)
        
        with col1:
            st.metric("Total Messages", st.session_state.total_messages)
        
        with col2:
            avg_words = sum(c['word_count'] for c in comments_list) / len(comments_list)
            st.metric("Average Word Count", f"{avg_words:.1f}")
        
        with col3:
            sentiments = [c['sentiment'] for c in comments_list]
            positive_pct = (sentiments.count('Positive') / len(sentiments)) * 100
            st.metric("Positive %", f"{positive_pct:.1f}%")
        
        with col4:
            if comments_list:
                st.metric("Last Update", comments_list[-1]['processed_time'])
    
        
        # Charts row
        col1, col2 = st.columns(2)
        
        with col1:
            # Sentiment distribution
            sentiment_counts = Counter(c['sentiment'] for c in comments_list)
            
            fig_pie = px.pie(
                values=list(sentiment_counts.values()),
                names=list(sentiment_counts.keys()),
                title="Sentiment Distribution (Recent Messages)",
                color_discrete_map={
                    'Positive': '#00CC96',
                    'Negative': '#EF553B',
                    'Neutral': '#636EFA'
                }
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Sentiment over time
            if st.session_state.processor.sentiment_history:
                sentiment_df = pd.DataFrame(list(st.session_state.processor.sentiment_history))
                
                fig_line = px.line(
                    sentiment_df,
                    x='time',
                    y='polarity',
                    title="Sentiment Polarity Over Time",
                    color_discrete_sequence=['#FF6B6B']
                )
                fig_line.update_layout(xaxis_title="Time", yaxis_title="Polarity Score")
                st.plotly_chart(fig_line, use_container_width=True)
        
        # Recent comments table
        st.subheader("Recent Comments")
        
        if comments_list:
            recent_comments = comments_list[-10:]
            
            df_display = pd.DataFrame([{
                'ID': c['comment_id'][:8] + '...',
                'Comment': c['comment_text'],
                'Sentiment': c['sentiment'],
                'Words': c['word_count'],
                'Time': c['processed_time']
            } for c in reversed(recent_comments)])
            
            st.dataframe(df_display, use_container_width=True)
        
    else:
        print("Dashboard not working")

if __name__ == "__main__":
    main()