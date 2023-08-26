from typing import List, Dict, Optional
import pandas as pd
import logging
from dotenv import load_dotenv
import os
# Load environment variables from .env file
load_dotenv()
# import sys
# print(sys.path)

# Import separated functions
from mastodon_api import fetch_mastodon_profiles
from db_operations import store_profiles_in_postgres_struct, read_from_postgres, store_sentiment_data
from text_processing import remove_html_tags, analyze_sentiment

# Retrieve environment variables
base_url = os.getenv("BASE_URL")
access_token = os.getenv("ACCESS_TOKEN")
db_params = {
    'host': os.getenv("DB_HOST"),
    'port': os.getenv("DB_PORT"),
    'dbname': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"),
    'password': os.getenv("DB_PASS")
}


def main(base_url: str, access_token: str, db_params: Dict[str, str], limit: int = 10):
    # Fetch user profiles for analysis
    usernames = ['popsci', 'cricket']

    profiles = fetch_mastodon_profiles(base_url, access_token, usernames)
    # print(profiles)
    # Store profiles in PostgreSQL
    store_profiles_in_postgres_struct(profiles, db_params)

    # Read and analyze posts
    df = read_from_postgres(db_params)
    df['clean_content'] = df['note_content'].apply(remove_html_tags)
    df['sentiment'] = df['clean_content'].apply(analyze_sentiment)

    store_sentiment_data(df, db_params)

    # Visualization and other steps...

# Execute the main function
main(base_url, access_token, db_params)

