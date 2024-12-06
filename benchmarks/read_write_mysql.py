import pandas as pd
import random
import mysql.connector
from datetime import datetime
import math
import time
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rootroot',
    'database': 'CS6400_1000'
}

EXPERIMENTS = [
    # {'writes': 9000, 'recs': 1000}
    {'writes': 5000, 'recs': 5000}
    # {'writes': 1000, 'recs': 9000}
]

def convert_ratings_file_to_list(ratings_file):
    
    df = pd.read_csv(ratings_file)
    ratings_list = df.to_dict(orient='records')
    return ratings_list


def get_most_rated_category(conn,user_id):

    cur = conn.cursor(dictionary=True)

    query = """
    WITH user_rated_businesses AS (
        SELECT r.business_id
        FROM ratings r
        WHERE r.user_id = %s
    ),
    business_categories AS (
        SELECT bc.business_id, bc.category_name
        FROM business_categories bc
        JOIN user_rated_businesses urb ON bc.business_id = urb.business_id
    )
    SELECT category_name, COUNT(*) AS rating_count
    FROM business_categories
    GROUP BY category_name
    ORDER BY rating_count DESC
    LIMIT 1;
    """
    cur.execute(query, (user_id,))
    result = cur.fetchone()
    
    if result:
        # print(result['category_name'])
        return result['category_name']
    else:
        return None

def _fetch_fallback_recommendations(conn, category, limit):
        """
        Fetch fallback recommendations based on objective criteria within the specified category.
        """
        cur = conn.cursor(dictionary=True)

        query = """
        -- Step 1: Get businesses in the given category
        WITH category_businesses AS (
            SELECT DISTINCT b.business_id, b.business_name, b.avg_rating, b.num_reviews
            FROM businesses b
            JOIN business_categories bc ON b.business_id = bc.business_id
            WHERE bc.category_name = %s
        )

        -- Step 2: Return businesses sorted by average rating and total ratings
        SELECT cb.business_name, cb.business_id, cb.num_reviews, cb.avg_rating
        FROM category_businesses cb
        ORDER BY cb.avg_rating DESC, cb.num_reviews DESC, cb.business_name ASC
        LIMIT %s;
        """

        cur.execute(query, (category, limit))
        results = cur.fetchall()
        return results
    
def _fetch_recommendations_user(conn, user_id, category, limit):
        """
        Fetch recommendations based on SIMILAR_TO relationships.
        """
        cur = conn.cursor(dictionary=True)

        query = """
        -- Step 1: Get businesses rated by target user (for later filtering)
        WITH user_rated_businesses AS (
            SELECT DISTINCT r.business_id
            FROM ratings r
            WHERE r.user_id = %s
        ),
        
        -- Step 2: Get similar users and their similarity scores
        similar_users AS (
            SELECT s.user_id_2 AS similar_user_id, s.similarity_score
            FROM user_similarity s
            WHERE s.user_id_1 = %s
            UNION
            SELECT s.user_id_1 AS similar_user_id, s.similarity_score
            FROM user_similarity s
            WHERE s.user_id_2 = %s
        ),

        -- Step 3: Pre-filter businesses by category
        category_filtered_businesses AS (
            SELECT DISTINCT bc.business_id
            FROM business_categories bc
            WHERE bc.category_name = %s
        ),

        -- Step 4: Get businesses rated by similar users in the filtered category
        similar_user_ratings AS (
            SELECT r.business_id, r.rating, su.similarity_score
            FROM ratings r
            JOIN similar_users su ON r.user_id = su.similar_user_id
            WHERE r.business_id IN (
                SELECT business_id FROM category_filtered_businesses
            ) AND r.business_id NOT IN (
                SELECT business_id FROM user_rated_businesses
            )
        )

        -- Step 5: Calculate weighted score (normalized), total ratings, and average rating for each business
        SELECT b.business_name, b.business_id, 
            -- SUM(sur.rating * sur.similarity_score) / SUM(sur.similarity_score) AS weighted_score,
            SUM(sur.rating * sur.similarity_score) AS weighted_score,
            COUNT(sur.rating) AS total_ratings,
            AVG(sur.rating) AS avg_rating
        FROM similar_user_ratings sur
        JOIN businesses b ON sur.business_id = b.business_id
        GROUP BY b.business_id, b.business_name
        ORDER BY weighted_score DESC, avg_rating DESC
        LIMIT %s;
        """

        cur.execute(query, (user_id, user_id, user_id, category, limit))
        results = cur.fetchall()
        return results

    
    
def fetch_user_pairs(conn, user_id, min_common_items):
    cur = conn.cursor(dictionary=True)

    query = """
    SELECT r1.user_id AS user1_id, 
        r2.user_id AS user2_id,
        GROUP_CONCAT(r1.rating) AS ratings1, 
        GROUP_CONCAT(r2.rating) AS ratings2
    FROM ratings r1
    JOIN ratings r2 
    ON r1.business_id = r2.business_id AND r1.user_id < r2.user_id
    WHERE r1.user_id = %s
    GROUP BY r1.user_id, r2.user_id
    HAVING COUNT(DISTINCT r1.business_id) >= %s;
    """
    cur.execute(query, (user_id, min_common_items))
    pairs = cur.fetchall()

    cur.close()
    return pairs


def calculate_cosine_similarity(ratings1, ratings2):
    dot_product = sum(r1 * r2 for r1, r2 in zip(ratings1, ratings2))
    magnitude1 = math.sqrt(sum(r1 ** 2 for r1 in ratings1))
    magnitude2 = math.sqrt(sum(r2 ** 2 for r2 in ratings2))

    if magnitude1 == 0 or magnitude2 == 0:
        return 0 

    return dot_product / (magnitude1 * magnitude2)

def insert_similarities(conn, similarities):
    if not similarities:
        return 

    cur = conn.cursor()

    query = """
    INSERT INTO user_similarity (user_id_1, user_id_2, similarity_score, common_rated_items, last_updated)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        similarity_score = VALUES(similarity_score),
        common_rated_items = VALUES(common_rated_items),
        last_updated = VALUES(last_updated);
    """

    data = [(sim['user1_id'], sim['user2_id'], sim['similarity'], sim['common_items'], sim['last_updated'])
            for sim in similarities]

    cur.executemany(query, data)
    conn.commit()

    logger.info(f"Processed batch with {len(similarities)} similarities")

    cur.close()
    
def calculate_similarity_for_affected_users(affected_users, min_common_items, min_similarity):
    connection = mysql.connector.connect(**DB_CONFIG)

    for user_id in affected_users:
        
        pairs = fetch_user_pairs(connection, user_id, min_common_items)

        similarities = []
        for pair in pairs:
            user2_id = pair['user2_id']
            ratings1 = list(map(float, pair['ratings1'].split(',')))
            ratings2 = list(map(float, pair['ratings2'].split(',')))

            
            similarity = calculate_cosine_similarity(ratings1, ratings2)

            if similarity >= min_similarity:
                similarities.append({
                    'user1_id': user_id,
                    'user2_id': user2_id,
                    'similarity': similarity,
                    'common_items': len(ratings1),
                    'last_updated': int(datetime.now().timestamp() * 1000)
                })

        
        insert_similarities(connection, similarities)

    connection.close()
    




def load_additional_ratings(ratings_entry):
    # print(ratings_entry)
    connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()

    

    
    # affected_users = set()

    
    user_data = []
    rating_data = []

    
    user_id = ratings_entry['user']
    print("user_id", user_id)
    
    business_id = ratings_entry['business']
    rating = ratings_entry['rating']
    timestamp = datetime.utcfromtimestamp(ratings_entry['timestamp'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

    user_data.append((user_id,))
    rating_data.append((business_id, user_id, rating, timestamp))

    user_query = "INSERT IGNORE INTO users (user_id) VALUES (%s)"
    cursor.executemany(user_query, user_data)

    rating_query = """
        INSERT INTO ratings (business_id, user_id, rating, timestamp)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE rating = VALUES(rating), timestamp = VALUES(timestamp)
    """
    cursor.executemany(rating_query, rating_data)

    connection.commit()


    cursor.close()
    connection.close()

    return user_id


def run_experiment(ratings_file, experiment_config):
    connection = mysql.connector.connect(**DB_CONFIG)

    affected_users = set()


    write_count = 0
    rec_count = 0

    writes = ['write'] * experiment_config['writes']
    recs = ['rec'] * experiment_config['recs']

    
    actions = writes + recs
    random.shuffle(actions)

    ratings_list = convert_ratings_file_to_list(ratings_file)
    # print(ratings_list)
    for action in actions:
        if action == 'write':
            
            affected_users.add(load_additional_ratings(ratings_list[write_count]))
            write_count += 1
            logger.info(f"Processed write #{write_count}")

        if write_count % 100 == 0:
            
            logger.info(f"Recalculating similarities after {write_count} writes")
            calculate_similarity_for_affected_users(list(affected_users), min_common_items=3, min_similarity=0.3)
            affected_users = set()  

        if action == 'rec':
            rec_count += 1
            if affected_users:
                user_id = random.choice(list(affected_users))
                category = get_most_rated_category(connection, user_id)
                
                results = _fetch_recommendations_user(connection, user_id, category, 5)
                if not results:
                    results = _fetch_fallback_recommendations(connection, category, 5)
            else:  
                results = _fetch_recommendations_user(connection, '108416619844777498346', 'Restaurant', 5)
            for idx, rec in enumerate(results):
                logger.info(f"{idx + 1}. {rec['business_name']} ({rec['business_id']})")
            logger.info(f"Processed recommendation #{rec_count}")

        

    logger.info(f"Completed {experiment_config['writes']} writes and {experiment_config['recs']} recs")

    connection.close()


def run_all_experiments(ratings_file):
    connection = mysql.connector.connect(**DB_CONFIG)

    
    results_time = []
    for experiment_config in EXPERIMENTS:
        
        logger.info(f"Running experiment with {experiment_config['writes']} writes and {experiment_config['recs']} recs")
        start_time = time.time()
        run_experiment(ratings_file, experiment_config)
        end_time = time.time()
        results_time.append(f"Time taken for {experiment_config['writes']} writes and {experiment_config['recs']} recs: " + str(end_time - start_time) + " seconds")
    for result in results_time:
        print(result)
    connection.close()

if __name__ == "__main__":
    additional_ratings_file = "data/benchmark/1k_9000_dummy_ratings.csv"
    run_all_experiments(additional_ratings_file)
