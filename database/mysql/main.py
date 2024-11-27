import ast
from datetime import datetime
import pandas as pd
import mysql.connector
import time

# Creates tables with hard-coded schema
def create_tables(conn):
    """Creates tables with the schemas specified in create_tables.sql"""

    with open("database/mysql/create_tables.sql", "r") as file:
        sql_script = file.read()
    
    # Execute queries iteratively
    for query in sql_script.split(";"):
        if query.strip():
            conn.cursor().execute(query)


# Loads a provided dataset
def load_dataset(conn, ratings_file, metadata_file):
    """
    Creates new tables and loads the provided ratings and metadata datasets.
    """

    # Recreate tables
    create_tables(conn)

    # Load business metadata first
    for chunk in pd.read_csv(metadata_file, chunksize=1000):
        for _, row in chunk.iterrows():
            # Load businesses
            query = (
                f"INSERT INTO businesses (business_id, business_name, avg_rating, num_reviews)"
                f"VALUES (%s, %s, %s, %s)"
            )
            conn.cursor().execute(
                query,
                (
                    row['gmap_id'],
                    row['name'],
                    row['avg_rating'],
                    row['num_of_reviews'],
                )
            )

            # Load business categories
            if row['category'] == row['category']:  # nan check
                for category in ast.literal_eval(row['category']):
                    query = (
                        f"INSERT INTO business_categories (business_id, category_name)"
                        f"VALUES (%s, %s)"
                    )
                    conn.cursor().execute(
                        query,
                        (
                            row['gmap_id'],
                            category
                        )
                    )

    # Load ratings and users data
    for chunk in pd.read_csv(ratings_file, chunksize=100):
        for _, row in chunk.iterrows():
            # Load user
            query = (
                f"INSERT IGNORE INTO users (user_id)"
                f"VALUES (%s)"
            )
            conn.cursor().execute(query, (row['user'],))

            # Convert timestamp into correct format
            dt_object = datetime.utcfromtimestamp(row['timestamp'] / 1000.0)
            formatted_datetime = dt_object.strftime('%Y-%m-%d %H:%M:%S')

            # Load rating
            query = (
                f"INSERT INTO ratings (business_id, user_id, rating, timestamp)"
                f"VALUES (%s, %s, %s, %s)"
            )
            conn.cursor().execute(
                query,
                (
                    row['business'],
                    row['user'],
                    row['rating'],
                    formatted_datetime,
                )
            )
        





    

if __name__ == "__main__":
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="cs6400",
            password="qwertyuiop",
            database="cs6400_0",
            autocommit=True
        )

        cursor = conn.cursor()
        print(cursor)

    except mysql.connector.Error as e:
        print(f"Error: {e}")
        exit()
        
    create_tables(conn)

    # Try loading the 100 data
    start_time = time.time()

    ratings_file = "data/samples/ratings_100_23844.csv"
    metadata_file = "data/samples/metadata_100_23844.csv"
    load_dataset(conn, ratings_file, metadata_file)

    end_time = time.time()

    print(f"Database loading time: {end_time - start_time} seconds")

    # Try loading the 1k data
    start_time = time.time()

    ratings_file = "data/samples/ratings_1000_170790.csv"
    metadata_file = "data/samples/metadata_1000_170790.csv"
    load_dataset(conn, ratings_file, metadata_file)

    end_time = time.time()

    print(f"Database loading time: {end_time - start_time} seconds")

    # Try loading the 10k data
    start_time = time.time()

    ratings_file = "data/samples/ratings_10000_1469798.csv"
    metadata_file = "data/samples/metadata_10000_1469798.csv"
    load_dataset(conn, ratings_file, metadata_file)

    end_time = time.time()

    print(f"Database loading time: {end_time - start_time} seconds")
