from neo4j import GraphDatabase
import random
import pandas as pd
import argparse
import ast
import logging
import time
from similarityCalculatorNoCache import SimilarityCalculatorNoCache

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
        
        
EXPERIMENTS = [
    # {'writes': 9000, 'recs': 1000}
    # {'writes': 5000, 'recs': 5000}
    {'writes': 1000, 'recs': 9000}
]

def convert_ratings_file_to_list(ratings_file):
    
    df = pd.read_csv(ratings_file)
    ratings_list = df.to_dict(orient='records')
    return ratings_list

def get_most_rated_category(conn, user_id):

    print(user_id)

    query = """
    MATCH (u:User {user_id: $user_id})-[:RATED]->(b:Business)-[:BELONGS_TO]->(c:Category)
    WITH c.name AS category_name, COUNT(*) AS rating_count
    ORDER BY rating_count DESC
    LIMIT 1
    RETURN category_name
    """

    
    result = conn.query(query, parameters={'user_id': user_id})

    print(result)
    if result:
        return result[0].get('category_name', None)
    else:
        return None

def _fetch_fallback_recommendations(conn, category, limit):
        """
        Fetch fallback recommendations based on objective criteria within the specified category.
        """
        query = """
        MATCH (b:Business)-[:BELONGS_TO]->(c:Category {name: $category})
        MATCH (b)<-[r:RATED]-()
        RETURN b.name AS business_name, b.gmap_id AS business_id, COUNT(r) AS total_ratings, AVG(r.rating) AS avg_rating
        ORDER BY avg_rating DESC, total_ratings DESC, b.name ASC
        LIMIT $limit
        """

       
        fallback_recommendations = conn.query(query, {
            'category': category,
            'limit': limit
        })
        return fallback_recommendations

def _fetch_recommendations_user(conn, user_id, category, limit):
        """
        Fetch recommendations based on SIMILAR_TO relationships.
        """
        query = """
        // Get similar users
        MATCH (u:User {user_id: $user_id})-[s:SIMILAR_TO]-(similar:User)
        WITH u, similar, s.score AS similarity_score

        // Get businesses rated by similar users
        MATCH (similar)-[r:RATED]->(b:Business)-[:BELONGS_TO]->(c:Category {name: $category})
        WHERE NOT EXISTS((u)-[:RATED]->(b))

        // Aggregate recommendations based on user similarity and ratings
        RETURN b.name AS business_name, b.gmap_id AS business_id,
               SUM(r.rating * similarity_score) AS weighted_score,
               COUNT(r) AS total_ratings, AVG(r.rating) AS avg_rating
        ORDER BY weighted_score DESC, avg_rating DESC
        LIMIT $limit
        """

        
        recommendations = conn.query(query, {'user_id': user_id, 'category': category, 'limit': limit})
        return recommendations
        
def load_additional_ratings_and_extract_affected_users(conn, ratings_entry):
    
    query = """
    MERGE (u:User {user_id: $user_id})
    WITH u
    MATCH (b:Business {gmap_id: $business_id})
    MERGE (u)-[r:RATED]->(b)
    SET r.rating = $rating,
        r.timestamp = $timestamp,
        r.last_updated = timestamp(),
        r.normalized_rating = CASE 
            WHEN $rating >= 4.5 THEN 5
            WHEN $rating >= 3.5 THEN 4
            WHEN $rating >= 2.5 THEN 3
            WHEN $rating >= 1.5 THEN 2
            ELSE 1
        END
    """
    params = {
        'user_id': ratings_entry['user'],
        'business_id': ratings_entry['business'],
        'rating': ratings_entry['rating'],
        'timestamp': ratings_entry['timestamp']
    }
    conn.query(query, params)


    return ratings_entry['user']

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
        
    def query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]





def run_experiment(ratings_file, experiment_config):
    conn = Neo4jConnection(
        uri="neo4j://localhost:7687",
        user="neo4j",
        password="neo4j@1234"
    )
    simCalc = SimilarityCalculatorNoCache(conn)
    affected_users = []


    write_count = 0
    rec_count = 0

    writes = ['write'] * experiment_config['writes']
    recs = ['rec'] * experiment_config['recs']


    actions = writes + recs
    random.shuffle(actions)

    ratings_list = convert_ratings_file_to_list(ratings_file)

    for action in actions:
        if action == 'write':

            affected_users.append(load_additional_ratings_and_extract_affected_users(conn, ratings_list[write_count]))
            write_count += 1
            logger.info(f"Processed write #{write_count}")
        
        if write_count % 100 == 0:
            simCalc.update_user_similarity(
                        affected_users=affected_users,
                        min_common_items=3,
                        min_similarity=0.3
                    )
            affected_users = []
            
        if action == 'rec':
            rec_count += 1
            if affected_users:
                user_id = random.choice(list(affected_users))
                category = get_most_rated_category(conn, user_id)
                results = _fetch_recommendations_user(conn, user_id,  category, 5)
                if not results:
                    results = _fetch_fallback_recommendations(conn,  category, 5)
            
            else:
                results = _fetch_recommendations_user(conn, '108416619844777498346', 'Restaurant', 5)
            
            for idx, rec in enumerate(results):
                logger.info(f"{idx + 1}. {rec['business_name']} ({rec['business_id']})")
            logger.info(f"Processed recommendation #{rec_count}")
            
    conn.close()

if __name__ == "__main__":
    
    results_time = []
    for experiment_config in EXPERIMENTS:
        
        logger.info(f"Running experiment with {experiment_config['writes']} writes and {experiment_config['recs']} recs")
        start_time = time.time()
        run_experiment("data/benchmark/10k_9000_dummy_ratings.csv", experiment_config)
        end_time = time.time()
        results_time.append(f"Time taken for {experiment_config['writes']} writes and {experiment_config['recs']} recs: " + str(end_time - start_time) + " seconds")
    for result in results_time:
        print(result)