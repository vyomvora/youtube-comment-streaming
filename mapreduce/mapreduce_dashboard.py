import streamlit as st
import boto3
import pandas as pd
import json
import io
from datetime import datetime

# Streamlit page configuration
st.set_page_config(page_title="MapReduce Results Dashboard", layout="wide")

# Initialize S3 client
s3_client = boto3.client('s3')

# Constants
S3_BUCKET = "youtube-comments-output"
S3_PREFIX = "mapreduce-results/"

def load_s3_file(filename, file_type="csv"):
    """Helper function to load files from S3"""
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}{filename}")
        if file_type == "csv":
            return pd.read_csv(io.BytesIO(obj['Body'].read()))
        elif file_type == "json":
            return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        st.error(f"Error loading {filename}: {e}")
        return None

# Main dashboard
st.title("MapReduce Results Dashboard")

# Sidebar for selecting results
st.sidebar.header("Select Results")
available_files = []
try:
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{S3_PREFIX}performance_metrics_")
    if 'Contents' in response:
        available_files = [
            obj['Key'].split('/')[-1] 
            for obj in response['Contents'] 
            if obj['Key'].endswith('.csv')
        ]
except Exception as e:
    st.error(f"Error listing S3 files: {e}")

selected_file = st.sidebar.selectbox(
    "Select Result Set",
    available_files,
    index=0 if available_files else None
)

if selected_file:
    # Extract timestamp from filename
    timestamp = selected_file.replace("performance_metrics_", "").replace(".csv", "")
    # Load benchmark summary
    summary = load_s3_file(f"benchmark_summary_{timestamp}.json", "json")
    # Load performance metrics
    perf_df = load_s3_file(f"performance_metrics_{timestamp}.csv", "csv")
    
    if perf_df is not None:
        st.header(f"Performance Metrics for {summary['num_processes']} processes")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Execution Times")
            st.bar_chart(perf_df[['method', 'time_seconds']].set_index('method'))
        
        with col2:
            st.subheader("Metrics Table")
            st.dataframe(
                perf_df[['method', 'time_seconds', 'unique_words', 'speedup', 'efficiency']],
                use_container_width=True
            )
    
    # Load top words
    top_words_df = load_s3_file(f"top_words_{timestamp}.csv", "csv")
    
    if top_words_df is not None:
        st.header("Top 5 Words")
        st.dataframe(
            top_words_df[['word', 'count', 'rank']].head(5),
            use_container_width=True
        )
    
    # Load sentiment analysis if available
    sentiment_df = load_s3_file(f"sentiment_analysis_{timestamp}.csv", "csv")
    
    if sentiment_df is not None:
        st.header("Sentiment Distribution")
        st.bar_chart(
            sentiment_df[['sentiment', 'count']].set_index('sentiment'),
            use_container_width=True
        )
    
    if summary:
        st.header("Summary")
        st.write(f"Processed {summary['total_comments']} comments")
        st.write(f"Timestamp: {datetime.strptime(summary['timestamp'], '%Y%m%d_%H%M%S').strftime('%Y-%m-%d %H:%M:%S')}")

else:
    st.warning("No results found in S3 bucket. Please run the MapReduce processor first.")