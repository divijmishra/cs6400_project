from datetime import datetime
import math
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from mysql.connector.pooling import MySQLConnectionPool

from database.mysql.mysqlconnection import MySQLConnection

# Configuration
MAX_WORKERS = 4  # Maximum number of threads
BATCH_SIZE = 100
MIN_COMMON_ITEMS = 3
MIN_SIMILARITY = 0.3

# Database details
HOST = "localhost"
USER = "cs6400"
PASSWORD = "qwertyuiop"

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection(num_businesses):
    return MySQLConnection(
                host=HOST,
                user=USER,
                password=PASSWORD,
                num_businesses=num_businesses
            ).connection

###############################################################
# USER SIMILARITY CALCULATION
###############################################################

# Fetch active users (users who have rated >= min_common_items businesses)
def fetch_active_users(min_common_items, num_businesses):
    conn = get_db_connection(num_businesses)
    cur = conn.cursor(dictionary=True)

    query = """
    SELECT u.user_id, COUNT(r.business_id) as rating_count
    FROM users u
    JOIN ratings r ON u.user_id = r.user_id
    GROUP BY u.user_id
    HAVING COUNT(r.business_id) >= %s
    ORDER BY COUNT(r.business_id) DESC;
    """
    cur.execute(query, (min_common_items,))
    active_users = cur.fetchall()

    cur.close()
    conn.close()
    return active_users

# Fetch relevant pairs for cosine similarity
def fetch_user_pairs(user_id, min_common_items, num_businesses):
    conn = get_db_connection(num_businesses)
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

# Calculate cosine similarity
def calculate_cosine_similarity(ratings1, ratings2):
    dot_product = sum(r1 * r2 for r1, r2 in zip(ratings1, ratings2))
    magnitude1 = math.sqrt(sum(r1 ** 2 for r1 in ratings1))
    magnitude2 = math.sqrt(sum(r2 ** 2 for r2 in ratings2))

    if magnitude1 == 0 or magnitude2 == 0:
        return 0  # Avoid division by zero

    return dot_product / (magnitude1 * magnitude2)

# Insert similarities into the database
def insert_user_similarities(conn, similarities):
    if not similarities:
        return  # Skip if no similarities to insert

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

# Process a batch of users
def process_user_batch(conn, user_batch, min_common_items, min_similarity, num_businesses):
    similarities = []

    for user_data in user_batch:
        user1_id = user_data['user_id']
        pairs = fetch_user_pairs(user1_id, min_common_items, num_businesses)

        for pair in pairs:
            user2_id = pair['user2_id']
            ratings1 = list(map(float, pair['ratings1'].split(',')))
            ratings2 = list(map(float, pair['ratings2'].split(',')))

            similarity = calculate_cosine_similarity(ratings1, ratings2)

            if similarity >= min_similarity:
                similarities.append({
                    'user1_id': user1_id,
                    'user2_id': user2_id,
                    'similarity': similarity,
                    'common_items': len(ratings1),
                    'last_updated': int(datetime.now().timestamp() * 1000)
                })

    # Insert calculated similarities into the database
    insert_user_similarities(conn, similarities)

# Main execution
def run_user_similarity_calculation(min_common_items, min_similarity, batch_size, num_businesses):
    start_time = time.time()
    active_users = fetch_active_users(min_common_items, num_businesses)
    print(f"Fetched {len(active_users)} active users")

    # Create user batches
    user_batches = [active_users[i:i + batch_size] for i in range(0, len(active_users), batch_size)]

    def worker(batch):
        conn = get_db_connection(num_businesses)
        try:
            process_user_batch(conn, batch, min_common_items, min_similarity, num_businesses)
        finally:
            conn.close()

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(worker, batch) for batch in user_batches]
        for future in futures:
            future.result()

    print("Completed processing all user similarities.")
    end_time = time.time()  # End timing
    time_taken = end_time - start_time
    logger.info(f"Time taken for similarity calculation: {time_taken:.2f} seconds")

###############################################################
# BUSINESS SIMILARITY CALCULATION
###############################################################

# Fetches all categories for each business
def fetch_businesses_with_categories(num_businesses):
    conn = get_db_connection(num_businesses)
    cur = conn.cursor(dictionary=True)

    query = """
    SELECT b.business_id, GROUP_CONCAT(DISTINCT bc.category_name) AS categories
    FROM businesses b
    JOIN business_categories bc ON b.business_id = bc.business_id
    GROUP BY b.business_id
    HAVING categories IS NOT NULL
    ORDER BY LENGTH(categories) DESC;
    """

    cur.execute(query)
    businesses = cur.fetchall()

    cur.close()
    conn.close()
    return businesses

# Calculates similarity score for a pair of businesses
def calculate_business_pair_similarity(b1, b2, min_similarity):
    categories1 = set(b1['categories'].split(','))
    categories2 = set(b2['categories'].split(','))

    intersection = len(categories1 & categories2)
    union = len(categories1 | categories2)

    if union > 0:
        similarity = intersection / union
        if similarity >= min_similarity:
            return {
                'business1_id': b1['business_id'],
                'business2_id': b2['business_id'],
                'similarity': similarity,
                'common_categories': intersection,
                'last_updated': int(datetime.now().timestamp() * 1000)
            }
    return None

def process_business_batch(business_batch, min_similarity, num_businesses):
    similarities = []

    for b1, b2 in business_batch:
        similarity = calculate_business_pair_similarity(b1, b2, min_similarity)
        if similarity:
            similarities.append(similarity)

    return similarities

def bulk_insert_similarities(similarities, num_businesses):
    if not similarities:
        return 

    conn = get_db_connection(num_businesses)
    cur = conn.cursor()

    query = """
    INSERT INTO business_similarity (business_id_1, business_id_2, similarity_score, common_categories, last_updated)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE 
        similarity_score = VALUES(similarity_score),
        common_categories = VALUES(common_categories),
        last_updated = VALUES(last_updated);
    """

    data = [(sim['business1_id'], sim['business2_id'], sim['similarity'], sim['common_categories'], sim['last_updated'])
            for sim in similarities]

    cur.executemany(query, data)
    conn.commit()

    logger.info(f"Inserted {len(similarities)} similarities into the database")

    cur.close()
    conn.close()

def run_business_similarity_calculation(min_similarity, batch_size, num_businesses):
    businesses = fetch_businesses_with_categories(num_businesses)
    logger.info(f"Fetched {len(businesses)} businesses")

    business_pairs = list(combinations(businesses, 2))
    logger.info(f"Generated {len(business_pairs)} business pairs")

    
    pair_batches = [business_pairs[i:i + batch_size] for i in range(0, len(business_pairs), batch_size)]

    def worker(batch):
        return process_business_batch(batch, min_similarity, num_businesses)

    similarities = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(worker, batch) for batch in pair_batches]
        for future in futures:
            similarities.extend(future.result())

    bulk_insert_similarities(similarities, num_businesses)

###############################################################
# MAIN
###############################################################

if __name__ == "__main__":
    # subsets = [100, 1000, 5000, 10000]
    subsets = [1000]

    # Calculate business similarities
    for num_businesses in subsets:
        print(f"Calculating business similarities for {num_businesses} businesses subset.")

        start_time = time.time()
        run_business_similarity_calculation(MIN_SIMILARITY, BATCH_SIZE, num_businesses)
        end_time = time.time()

        print(f"Calculated user similarities for {num_businesses} businesses subset in {end_time - start_time} s.")
    
    # Calculate user similarities
    for num_businesses in subsets:
        print(f"Calculating user similarities for {num_businesses} businesses subset.")

        start_time = time.time()
        run_user_similarity_calculation(MIN_COMMON_ITEMS, MIN_SIMILARITY, BATCH_SIZE, num_businesses)
        end_time = time.time()

        print(f"Calculated user similarities for {num_businesses} businesses subset in {end_time - start_time} s.")

    