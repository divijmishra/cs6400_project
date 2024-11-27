import ast
from datetime import datetime
import pandas as pd
import mysql.connector
import time
from database.mysql.mysqlconnection import MySQLConnection

# Creates tables with hard-coded schema
def create_tables(db: MySQLConnection):
    """Creates tables with the schemas specified in create_tables.sql"""

    connection = db.connection
    cursor = db.cursor

    with open("database/mysql/create_tables.sql", "r") as file:
        sql_script = file.read()
    
    # Execute queries iteratively
    for query in sql_script.split(";"):
        if query.strip():
            cursor.execute(query)


# Loads a provided dataset
def load_dataset(db: MySQLConnection):
    """
    Creates new tables and loads the provided ratings and metadata datasets.
    """

    connection = db.connection
    cursor = db.cursor

    ratings_file = f"data/samples/ratings_{db.num_businesses}.csv"
    metadata_file = f"data/samples/metadata_{db.num_businesses}.csv"

    # Recreate tables
    create_tables(db)

    # Load business metadata first
    for chunk in pd.read_csv(metadata_file, chunksize=1000):
        business_data = []
        category_data = []

        for _, row in chunk.iterrows():
            # Load businesses
            business_data.append(
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
                    category_data.append(
                        (
                            row['gmap_id'],
                            category
                        )
                    )
        
        # Load business data
        query = (
            f"INSERT INTO businesses (business_id, business_name, avg_rating, num_reviews)"
            f"VALUES (%s, %s, %s, %s)"
        )
        cursor.executemany(query, business_data)

        # Load category data
        query = (
            f"INSERT INTO business_categories (business_id, category_name)"
            f"VALUES (%s, %s)"
        )
        cursor.executemany(query, category_data)

    # Load ratings and users data
    for chunk in pd.read_csv(ratings_file, chunksize=1000):
        user_data = []
        rating_data = []
        for _, row in chunk.iterrows():
            # Load user
            user_data.append((row['user'],))

            # Convert timestamp into correct format
            dt_object = datetime.utcfromtimestamp(row['timestamp'] / 1000.0)
            formatted_datetime = dt_object.strftime('%Y-%m-%d %H:%M:%S')

            # Load rating
            rating_data.append(
                (
                    row['business'],
                    row['user'],
                    row['rating'],
                    formatted_datetime,
                )
            )

        # Load users
        query = (
            f"INSERT IGNORE INTO users (user_id)"
            f"VALUES (%s)"
        )
        cursor.executemany(query, user_data)

        # Load ratings
        query = (
            f"INSERT INTO ratings (business_id, user_id, rating, timestamp)"
            f"VALUES (%s, %s, %s, %s)"
        )
        cursor.executemany(query, rating_data)

    connection.commit()


if __name__ == "__main__":
    subsets = [100, 1000]

    for num_businesses in subsets:
        try:
            db = MySQLConnection(
                host="localhost",
                user="cs6400",
                password="qwertyuiop",
                num_businesses=num_businesses
            )

        except mysql.connector.Error as e:
            print(f"Error: {e}")
            exit()

        print(f"Loading databases for sample with {num_businesses} businesses")

        start_time = time.time()
        load_dataset(db)
        end_time = time.time()

        print(f"Database loading time: {end_time - start_time} seconds")
        