import requests
import json
import pandas as pd
import psycopg2
from typing import List, Dict
import matplotlib.pyplot as plt

def fetch_mastodon_data(base_url: str, access_token: str, limit: int = 10):
        headers = {'Authorization': f'Bearer {access_token}'}
        params = {'limit': limit}
        response = requests.get(f'{base_url}/api/v1/timelines/public', headers=headers, params=params)
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            print(f"Failed to fetch data: {response.status_code}")
            return None

def store_in_postgres(data: List[Dict], db_params: Dict[str, str]):
    try:
        connection = psycopg2.connect(
            host=db_params['host'],
            port=db_params['port'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password']
        )
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS toots (id BIGINT PRIMARY KEY, content TEXT);''')
        for entry in data:
            cursor.execute("INSERT INTO toots (id, content) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;", (entry['id'], entry['content']))
        connection.commit()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

def read_from_postgres(db_params: Dict[str, str]):
    try:
        connection = psycopg2.connect(
            host=db_params['host'],
            port=db_params['port'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password']
        )
        df = pd.read_sql_query("SELECT * FROM toots", connection)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()
    return df

def visualize_data(df: pd.DataFrame):
    df['word_count'] = df['content'].apply(lambda x: len(str(x).split(" ")))
    plt.hist(df['word_count'], bins=20)
    plt.xlabel('Word Count')
    plt.ylabel('Frequency')
    plt.title('Word Count Distribution in Toots')
    plt.show()


# Main function to orchestrate the pipeline
def main(base_url: str, access_token: str, db_params: Dict[str, str], limit: int = 10):

    data = fetch_mastodon_data(base_url, access_token, limit)
    if data is None:
        return

    # Step 2: Store Data in PostgreSQL
    store_in_postgres(data, db_params)

    # Step 3: Read Data from PostgreSQL into Pandas DataFrame
    df = read_from_postgres(db_params)

    # Step 4: Visualize Data
    visualize_data(df)

# Database Parameters
db_params = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'lp_database_testing',
    'user': 'username',
    'password': 'password'
}

# API Parameters
base_url = "https://mastodon.social"
access_token = "Is0UxBl-x9GZoUdsJJ7OYlGNnKQTXpnkO1gXipGinxw"

# Execute the main function
main(base_url, access_token, db_params)
