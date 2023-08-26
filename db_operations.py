from typing import List, Dict, Optional
import psycopg2
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


def store_profiles_in_postgres_unstruct(data: List[Dict], db_params: Dict[str, str]):
    connection: Optional[psycopg2.extensions.connection] = None
    cursor: Optional[psycopg2.extensions.cursor] = None
    try:
        connection = psycopg2.connect(
            host=db_params['host'],
            port=db_params['port'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password']
        )
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS profiles (id BIGINT PRIMARY KEY, username TEXT);''')
        for entry in data:
            cursor.execute("INSERT INTO profiles (id, username) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING;", 
                           (entry.get('id'), entry.get('username')))
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def store_profiles_in_postgres_struct(data: List[Dict], db_params: Dict[str, str]) -> None:
    connection: Optional[psycopg2.extensions.connection] = None
    cursor: Optional[psycopg2.extensions.cursor] = None
    try:
        connection = psycopg2.connect(
            host=db_params['host'],
            port=db_params['port'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password']
        )
        cursor = connection.cursor()

        drop_table_query = '''DROP TABLE IF EXISTS profiles;'''

        cursor.execute(drop_table_query)

        create_table_query = '''CREATE TABLE IF NOT EXISTS profiles (
            id BIGINT PRIMARY KEY,
            username TEXT,
            display_name TEXT,
            locked BOOLEAN,
            created_at TIMESTAMP,
            followers_count INT,
            following_count INT,
            statuses_count INT,
            last_status_at TIMESTAMP,
            other_data JSONB,
            dw_refresh_time TIMESTAMP
        );'''

        cursor.execute(create_table_query)

        for entry in data:
            current_time = datetime.now()
            insert_query = '''INSERT INTO profiles (
                id, username, display_name, locked, created_at, followers_count, following_count, statuses_count, last_status_at, other_data, dw_refresh_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;'''

            cursor.execute(insert_query, (
                entry.get('id'),
                entry.get('username'),
                entry.get('display_name'),
                entry.get('locked'),
                entry.get('created_at'),
                entry.get('followers_count'),
                entry.get('following_count'),
                entry.get('statuses_count'),
                entry.get('last_status_at'),
                json.dumps(entry),  # Store the complete JSON as well
                current_time
            ))

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# METHOD 1 = setting conn using psycopg3

def read_from_postgres(db_params: Dict[str, str]):
    try:
        connection = psycopg2.connect(
            host=db_params['host'],
            port=db_params['port'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password']
        )
        df = pd.read_sql_query("SELECT *, other_data->>'note' as note_content FROM profiles", connection)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()
    return df

# METHOD 2 - setting conn using SQLAlchemy

# from sqlalchemy import create_engine
# import pandas as pd
# from typing import Dict, Optional

# def read_from_postgres(db_params: Dict[str, str]) -> Optional[pd.DataFrame]:
#     df = None  # Initialize df to None
#     engine = None  # Initialize engine to None
#     try:
#         # Create an SQLAlchemy engine
#         engine = create_engine(f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}")

#         # Use the engine connection with Pandas
#         df = pd.read_sql_query("SELECT *, other_data->>'note' as note_content FROM profiles", engine)
#     except Exception as e:
#         print(f"Error: {e}")
#     finally:
#         if engine is not None:
#             engine.dispose()  # Close the connection
#     return df


def store_sentiment_data(df: pd.DataFrame, db_params: Dict[str, str]) -> None:
    connection: Optional[psycopg2.extensions.connection] = None
    cursor: Optional[psycopg2.extensions.cursor] = None
    try:
        connection = psycopg2.connect(
            host=db_params['host'],
            port=db_params['port'],
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password']
        )
        cursor = connection.cursor()

        # Create table if it doesn't exist
        drop_table_query = '''
        DROP TABLE IF EXISTS sentiment_data;'''

        cursor.execute(drop_table_query)

        # Create table if it doesn't exist
        create_table_query = '''
        CREATE TABLE IF NOT EXISTS sentiment_data (
            id BIGINT PRIMARY KEY,
            username TEXT,
            clean_content TEXT,
            sentiment TEXT,
            dw_refresh_time TIMESTAMP
        );'''

        cursor.execute(create_table_query)

        current_time = datetime.now()

        # Insert data
        for _, row in df.iterrows():
            insert_query = '''
            INSERT INTO sentiment_data (id, username, clean_content, sentiment, dw_refresh_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE
            SET clean_content = EXCLUDED.clean_content,
                sentiment = EXCLUDED.sentiment,
                dw_refresh_time = EXCLUDED.dw_refresh_time;
            '''
            cursor.execute(insert_query, (row['id'], row['username'], row['clean_content'], row['sentiment'], current_time))

        connection.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
