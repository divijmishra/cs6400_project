import time
import neo4j.exceptions
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from pathlib import Path
import traceback
import random
from neo4j_connection import Neo4jConnection

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SimilarityCalculatorNoCache:
    def __init__(self, conn):
        self.conn = conn
        self.setup_indexes()
    
    def setup_indexes(self):
        indexes = [
            "CREATE INDEX user_id IF NOT EXISTS FOR (u:User) ON (u.user_id)",
            "CREATE INDEX business_gmap_id IF NOT EXISTS FOR (b:Business) ON (b.gmap_id)",
            "CREATE INDEX category_name IF NOT EXISTS FOR (c:Category) ON (c.name)",
            "CREATE INDEX rating_index IF NOT EXISTS FOR ()-[r:RATED]-() ON (r.rating)",
            "CREATE INDEX similarity_score IF NOT EXISTS FOR ()-[s:SIMILAR_TO]-() ON (s.score)",
            "CREATE INDEX similarity_last_updated IF NOT EXISTS FOR ()-[s:SIMILAR_TO]-() ON (s.last_updated)"
        ]
        
        with self.conn.driver.session() as session:
            for index_query in indexes:
                try:
                    session.run(index_query)
                    logger.info(f"Successfully created index: {index_query}")
                except Exception as e:
                    logger.warning(f"Error creating index {index_query}: {e}")

    def _calculate_cosine_similarity(self, vector1, vector2):
        """Calculate cosine similarity between two vectors."""
        try:
            v1 = np.array(vector1)
            v2 = np.array(vector2)
            norm1 = np.linalg.norm(v1)
            norm2 = np.linalg.norm(v2)
            
            if norm1 == 0 or norm2 == 0:
                return 0
            
            return np.dot(v1, v2) / (norm1 * norm2)
        except Exception as e:
            logger.error(f"Similarity calculation error: {e}")
            return 0

    def query_retry(self, session, query, parameters, max_retries=5):
        for attempt in range(max_retries):
            try:
                return session.run(query, parameters)
            except neo4j.exceptions.TransientError as e:
                if "DeadlockDetected" not in str(e):
                    raise
                
                # Exponential backoff with jitter
                wait_time = min(2 ** attempt + random.random(), 30)
                logger.warning(f"Deadlock detected. Retry {attempt + 1}/{max_retries}. Waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
        
        raise Exception("Max retries reached due to persistent deadlocks")

    def calculate_user_similarity(self, min_common_items=3, min_similarity=0.3, batch_size=500):
        logger.info("Starting user similarity calculation...")

        # Delete existing SIMILAR_TO relationships for users
        delete_query = "MATCH (:User)-[s:SIMILAR_TO]->(:User) DELETE s"
        with self.conn.driver.session() as session:
            try:
                session.run(delete_query)
                logger.info("Deleted existing SIMILAR_TO relationships for users")
            except Exception as e:
                logger.error(f"Error deleting SIMILAR_TO relationships: {e}")

        start_time = time.time()

        # Get active users with sufficient ratings
        active_users_query = """
        MATCH (u:User)-[r:RATED]->(b:Business)
        WITH u, COUNT(r) as rating_count
        WHERE rating_count >= $min_common_items
        RETURN u.user_id as user_id, rating_count
        ORDER BY rating_count DESC
        """
        
        with self.conn.driver.session() as session:
            active_users = session.run(active_users_query, {'min_common_items': min_common_items})
            active_users = [record.data() for record in active_users]
        
        logger.info(f"Found {len(active_users)} active users")

        def process_user_batch(batch):
            """Process a batch of users and calculate their similarities"""
            batch_similarities = []
            
            with self.conn.driver.session() as session:
                for user_data in batch:
                    user1_id = user_data['user_id']
                    
                    pairs_query = """
                    MATCH (u1:User {user_id: $user1_id})-[r1:RATED]->(b:Business)<-[r2:RATED]-(u2:User)
                    WHERE u2.user_id > $user1_id
                    WITH u1, u2, 
                         COUNT(b) as common_items,
                         COLLECT({rating1: r1.rating, rating2: r2.rating}) as ratings
                    WHERE common_items >= $min_common_items AND u1.user_id < u2.user_id
                    RETURN u2.user_id as user2_id, ratings, common_items
                    """
                    
                    pairs = session.run(pairs_query, {
                        'user1_id': user1_id,
                        'min_common_items': min_common_items
                    })
                    
                    for record in pairs:
                        user2_id = record['user2_id']
                        ratings = record['ratings']
                        common_items = record['common_items']
                        
                        vector1 = [r['rating1'] for r in ratings]
                        vector2 = [r['rating2'] for r in ratings]
                        
                        similarity = self._calculate_cosine_similarity(vector1, vector2)
                        
                        if similarity >= min_similarity:
                            batch_similarities.append({
                                'user1_id': user1_id,
                                'user2_id': user2_id,
                                'similarity': float(similarity),
                                'common_items': common_items,
                                'last_updated': int(datetime.now().timestamp() * 1000)
                            })
                
                if batch_similarities:
                    bulk_upsert_query = """
                    UNWIND $similarities AS sim
                    MATCH (u1:User {user_id: sim.user1_id})
                    MATCH (u2:User {user_id: sim.user2_id})
                    MERGE (u1)-[s:SIMILAR_TO]-(u2)
                    SET s.score = sim.similarity,
                        s.common_items = sim.common_items,
                        s.last_updated = sim.last_updated
                    """
                    
                    try:
                        self.query_retry(session, bulk_upsert_query, {'similarities': batch_similarities})
                        logger.info(f"Processed batch with {len(batch_similarities)} similarities")
                    except Exception as e:
                        logger.error(f"Batch processing error: {e}")
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            for i in range(0, len(active_users), batch_size):
                batch = active_users[i:i + batch_size]
                executor.submit(process_user_batch, batch)
        
        end_time = time.time()
        logger.info(f"User similarity calculation took {end_time - start_time:.2f} seconds")
        logger.info("User similarity calculation completed")

    def calculate_business_similarity(self, min_similarity=0.3, batch_size=500):
        logger.info("Starting business similarity calculation...")

        # Delete existing SIMILAR_TO relationships for businesses
        delete_query = "MATCH (:Business)-[s:SIMILAR_TO]-(:Business) DELETE s"
        with self.conn.driver.session() as session:
            try:
                session.run(delete_query)
                logger.info("Deleted existing SIMILAR_TO relationships for businesses")
            except Exception as e:
                logger.error(f"Error deleting SIMILAR_TO relationships: {e}")
        
        start_time = time.time()

        # Get businesses with their categories
        get_businesses_query = """
        MATCH (b:Business)-[:BELONGS_TO]->(c:Category)
        WITH b, COLLECT(DISTINCT c.name) as categories
        WHERE SIZE(categories) > 0
        RETURN b.gmap_id as business_id, categories
        ORDER BY SIZE(categories) DESC
        """
        
        with self.conn.driver.session() as session:
            businesses = session.run(get_businesses_query)
            businesses = [record.data() for record in businesses]
        
        logger.info(f"Found {len(businesses)} businesses")
        
        def process_business_batch(batch, start_index):
            """Process a batch of businesses and calculate their similarities"""
            batch_similarities = []
            
            with self.conn.driver.session() as session:
                for i, b1 in enumerate(batch):
                    for b2 in businesses[start_index+i+1:]:
                        if(b1['business_id'] == b2['business_id']):
                            continue
                        # Jaccard similarity for categories
                        intersection = len(set(b1['categories']) & set(b2['categories']))
                        union = len(set(b1['categories']) | set(b2['categories']))
                        
                        if union == 0:
                            continue
                        
                        similarity = intersection / union
                        
                        if similarity >= min_similarity:
                            batch_similarities.append({
                                'business1_id': b1['business_id'],
                                'business2_id': b2['business_id'],
                                'similarity': similarity,
                                'common_categories': intersection,
                                'last_updated': int(datetime.now().timestamp() * 1000)
                            })
                
                if batch_similarities:
                    bulk_upsert_query = """
                    UNWIND $similarities AS sim
                    MATCH (b1:Business {gmap_id: sim.business1_id})
                    MATCH (b2:Business {gmap_id: sim.business2_id})
                    MERGE (b1)-[s:SIMILAR_TO]-(b2)
                    SET s.score = sim.similarity,
                        s.common_categories = sim.common_categories,
                        s.last_updated = sim.last_updated
                    """
                    
                    try:
                        self.query_retry(session, bulk_upsert_query, {'similarities': batch_similarities})
                        logger.info(f"Processed batch with {len(batch_similarities)} business similarities")
                    except Exception as e:
                        logger.error(f"Batch processing error: {e}")
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            for i in range(0, len(businesses), batch_size):
                batch = businesses[i:i + batch_size]
                executor.submit(process_business_batch, batch, i)

        end_time = time.time()
        print(f"Business similarity calculation took {end_time - start_time:.2f} seconds")
        logger.info("Business similarity calculation completed")
    
    def update_user_similarity(self, affected_users, min_common_items=3, min_similarity=0.3, batch_size=500):
        """
        Incrementally update user similarity based on affected users.
        """
        logger.info("Starting incremental user similarity update...")

        start_time = time.time()

        def process_user_batch(batch):
            """Process a batch of affected users and update their similarities."""
            batch_similarities = []

            with self.conn.driver.session() as session:
                for user1_id in batch:
                    # Find potential similarity pairs for the affected user
                    pairs_query = """
                    MATCH (u1:User {user_id: $user1_id})-[r1:RATED]->(b:Business)<-[r2:RATED]-(u2:User)
                    WITH u1, u2,
                        COUNT(b) as common_items,
                        COLLECT({rating1: r1.rating, rating2: r2.rating}) as ratings
                    WHERE common_items >= $min_common_items AND u1.user_id < u2.user_id
                    RETURN u2.user_id as user2_id, ratings, common_items
                    """
                    pairs = session.run(pairs_query, {
                        'user1_id': user1_id,
                        'min_common_items': min_common_items
                    })

                    for record in pairs:
                        user2_id = record['user2_id']
                        ratings = record['ratings']
                        common_items = record['common_items']

                        # Compute cosine similarity
                        vector1 = [r['rating1'] for r in ratings]
                        vector2 = [r['rating2'] for r in ratings]
                        similarity = self._calculate_cosine_similarity(vector1, vector2)

                        if similarity >= min_similarity:
                            batch_similarities.append({
                                'user1_id': user1_id,
                                'user2_id': user2_id,
                                'similarity': float(similarity),
                                'common_items': common_items,
                                'last_updated': int(datetime.now().timestamp() * 1000)
                            })

                if batch_similarities:
                    bulk_upsert_query = """
                    UNWIND $similarities AS sim
                    MATCH (u1:User {user_id: sim.user1_id})
                    MATCH (u2:User {user_id: sim.user2_id})
                    MERGE (u1)-[s:SIMILAR_TO]-(u2)
                    SET s.score = sim.similarity,
                        s.common_items = sim.common_items,
                        s.last_updated = sim.last_updated
                    """
                    try:
                        self.query_retry(session, bulk_upsert_query, {'similarities': batch_similarities})
                        logger.info(f"Processed batch with {len(batch_similarities)} similarities")
                    except Exception as e:
                        logger.error(f"Batch processing error: {e}")

        with ThreadPoolExecutor(max_workers=4) as executor:
            for i in range(0, len(affected_users), batch_size):
                batch = affected_users[i:i + batch_size]
                executor.submit(process_user_batch, batch)

        end_time = time.time()
        logger.info(f"Incremental user similarity update took {end_time - start_time:.2f} seconds")
        logger.info("Incremental user similarity update completed")


def main():
    conn = Neo4jConnection(
        uri="neo4j://localhost:7687",
        user="neo4j",
        password="qwertyuiop"
    )

    simCalc = SimilarityCalculatorNoCache(conn)

    try:
        # Calculate similarities
        simCalc.calculate_user_similarity()
        simCalc.calculate_business_similarity()    
    except Exception as e:
        logger.error(f"Similarity generation process failed: {e}")
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()