import logging
import time

from database.mysql.mysqlconnection import MySQLConnection

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

class MySQLRecommendationEngine:
    def __init__(self, conn):
        self.conn = conn

    def get_recommendations(self, user_id, category, limit=10):
        """
        Get collaborative filtering recommendations with category filtering.
        Falls back to objective recommendations within the same category if no results are found.
        """
        recommendations = self._fetch_recommendations(user_id, category, limit)

        # Fallback if no recommendations found
        if not recommendations:
            print("No recommendations found with the collaborative filter. Returning fallback recommendations.")
            recommendations = self._fetch_fallback_recommendations(category, limit)

        return recommendations
    
    def _fetch_recommendations(self, user_id, category, limit):
        """
        Fetch recommendations based on collaborative filtering with category filtering.
        """
        cur = self.conn.cursor(dictionary=True)

        query = """
        -- Step 1: Get all businesses rated by the target user
        WITH user_rated_businesses AS (
            SELECT DISTINCT r.business_id
            FROM ratings r
            WHERE r.user_id = %s
        ),

        -- Step 2: Find all users who rated the same businesses as the target user
        similar_users AS (
            SELECT DISTINCT r.user_id
            FROM ratings r
            JOIN user_rated_businesses ur
            ON r.business_id = ur.business_id
            WHERE r.user_id != %s
        ),
        
        -- Step 3: Find businesses rated by these similar users
        business_rated_by_similar_users AS (
            SELECT DISTINCT r.business_id AS other_business_id
            FROM ratings r
            JOIN similar_users su
            ON r.user_id = su.user_id
        ),

        -- Step 4: Filter businesses by category and exclude those already rated by the target user
        category_filtered_recommendations AS (
            SELECT brsu.other_business_id, COUNT(*) AS score
            FROM business_rated_by_similar_users brsu
            JOIN business_categories bc
            ON brsu.other_business_id = bc.business_id
            WHERE bc.category_name = %s
            AND brsu.other_business_id NOT IN (SELECT business_id FROM user_rated_businesses)
            GROUP BY brsu.other_business_id
        )

        -- Step 5: Return the top recommendations sorted by score
        SELECT b.business_name, b.business_id, cfr.score
        FROM category_filtered_recommendations cfr
        JOIN businesses b
        ON cfr.other_business_id = b.business_id
        ORDER BY cfr.score DESC
        LIMIT %s;
        """

        cur.execute(query, (user_id, user_id, category, limit))
        results = cur.fetchall()
        return results
    
    def _fetch_fallback_recommendations(self, category, limit):
        """
        Fetch fallback recommendations based on objective criteria within the specified category.
        """
        cur = self.conn.cursor(dictionary=True)

        query = """
        -- Step 1: Get businesses in the given category
        WITH category_businesses AS (
            SELECT DISTINCT b.business_id, b.business_name, b.avg_rating, b.num_reviews
            FROM businesses b
            JOIN business_categories bc ON b.business_id = bc.business_id
            WHERE bc.category_name = %s
        )

        -- Step 2: Return businesses sorted by average rating and total ratings
        SELECT cb.business_name, cb.business_id, cb.avg_rating, cb.num_reviews
        FROM category_businesses cb
        ORDER BY cb.avg_rating DESC, cb.num_reviews DESC
        LIMIT %s;
        """

        cur.execute(query, (category, limit))
        results = cur.fetchall()
        return results
    
    def _fetch_recommendations_user(self, user_id, category, limit):
        """
        Fetch recommendations based on SIMILAR_TO relationships.
        """
        cur = self.conn.cursor(dictionary=True)

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
            SELECT bc.business_id
            FROM business_categories bc
            WHERE bc.category_name = %s
        ),

        -- Step 4: Get businesses rated by similar users in the filtered category
        similar_user_ratings AS (
            SELECT r.business_id, r.rating, su.similarity_score
            FROM ratings r
            JOIN similar_users su ON r.user_id = su.similar_user_id
            WHERE r.business_id IN category_filtered_businesses
        )

        -- Step 5: Calculate weighted score (normalized), total ratings, and average rating for each business
        SELECT b.business_name, b.business_id, 
            SUM(sur.rating * sur.similarity_score) / SUM(sur.similarity_score) AS weighted_score,
            COUNT(sur.rating) AS total_ratings,
            AVG(sur.rating) AS avg_rating
        FROM similar_user_ratings sur
        JOIN businesses b ON sur.business_id = b.business_id
        WHERE b.business_id NOT IN user_rated_businesses
        GROUP BY b.business_id, b.business_name
        ORDER BY weighted_score DESC, avg_rating DESC
        LIMIT %s;
        """

        cur.execute(query, (user_id, user_id, category, user_id, limit))
        results = cur.fetchall()
        return results
    
    def _fetch_recommendations_user_business(self, user_id, category, limit):
        """
        Fetch recommendations based on user-user and business-business SIMILAR_TO relationships.
        """
        cur = self.conn.cursor(dictionary=True)

        query = """
        WITH similar_users AS (
            -- Get similar users and their similarity scores
            SELECT s.user_id_2 AS similar_user_id, s.similarity_score
            FROM user_similarity s
            WHERE s.user_id_1 = %s
            UNION
            SELECT s.user_id_1 AS similar_user_id, s.similarity_score
            FROM user_similarity s
            WHERE s.user_id_2 = %s
        ),
        user_based_ratings AS (
            -- Get businesses rated by similar users in the given category
            SELECT r.business_id, r.rating, su.similarity_score
            FROM ratings r
            JOIN similar_users su ON r.user_id = su.similar_user_id
            JOIN business_categories bc ON r.business_id = bc.business_id
            WHERE bc.category_name = %s
        ),
        business_based_ratings AS (
            -- Get businesses similar to those already rated by the user
            SELECT r.business_id, r.rating, su.similarity_score
            FROM ratings r
            JOIN business_categories bc ON r.business_id = bc.business_id
            JOIN user_similarity su ON r.user_id = su.user_id_2
            WHERE su.user_id_1 = %s
            AND NOT EXISTS (
                SELECT 1
                FROM ratings r_user
                WHERE r_user.user_id = %s
                AND r_user.business_id = r.business_id
            )
        )
        -- Combine scores from user-user and business-business similarity
        SELECT b.business_name, b.business_id, 
            -- Normalize weighted score by dividing the sum of ratings by the sum of similarity scores
            COALESCE(SUM(ubr.rating * ubr.similarity_score) / NULLIF(SUM(ubr.similarity_score), 0), 0) AS user_based_score,
            COALESCE(SUM(bbr.rating * bbr.similarity_score) / NULLIF(SUM(bbr.similarity_score), 0), 0) AS business_based_score,
            COALESCE(COUNT(ubr.rating), 0) AS total_ratings,
            COALESCE(AVG(ubr.rating), 0) AS avg_rating
        FROM businesses b
        LEFT JOIN user_based_ratings ubr ON b.business_id = ubr.business_id
        LEFT JOIN business_based_ratings bbr ON b.business_id = bbr.business_id
        GROUP BY b.business_id, b.business_name
        ORDER BY (user_based_score + business_based_score) DESC, avg_rating DESC
        LIMIT %s;
        """

        cur.execute(query, (user_id, user_id, category, user_id, user_id, limit))
        results = cur.fetchall()
        return results
    
    def get_recommendations_bayesian(self, user_id, category, limit=10):
        """
        Get recommendations using Bayesian Averaging for confidence-based scoring.
        """
        cur = self.conn.cursor(dictionary=True)

        query = """
        WITH similar_users AS (
            -- Get similar users and their similarity scores
            SELECT s.user_id_2 AS similar_user_id, s.similarity_score
            FROM user_similarity s
            WHERE s.user_id_1 = %s
            UNION
            SELECT s.user_id_1 AS similar_user_id, s.similarity_score
            FROM user_similarity s
            WHERE s.user_id_2 = %s
        ),
        user_based_ratings AS (
            -- Get businesses rated by similar users in the given category
            SELECT r.business_id, r.rating, su.similarity_score
            FROM ratings r
            JOIN similar_users su ON r.user_id = su.similar_user_id
            JOIN business_categories bc ON r.business_id = bc.business_id
            WHERE bc.category_name = %s
        ),
        business_based_ratings AS (
            -- Get businesses similar to those already rated by the user
            SELECT r.business_id, r.rating, su.similarity_score
            FROM ratings r
            JOIN business_categories bc ON r.business_id = bc.business_id
            JOIN user_similarity su ON r.user_id = su.user_id_2
            WHERE su.user_id_1 = %s
            AND NOT EXISTS (
                SELECT 1
                FROM ratings r_user
                WHERE r_user.user_id = %s
                AND r_user.business_id = r.business_id
            )
        )
        -- Combine scores from user-user and business-business similarity
        SELECT b.business_name, b.business_id, 
            -- Calculate user-based score (weighted by similarity score)
            COALESCE(SUM(ubr.rating * ubr.similarity_score), 0) / COALESCE(SUM(ubr.similarity_score), 1) AS user_based_score,
            -- Calculate business-based score (weighted by similarity score)
            COALESCE(SUM(bbr.rating * bbr.similarity_score), 0) / COALESCE(SUM(bbr.similarity_score), 1) AS business_based_score,
            COALESCE(COUNT(ubr.rating), 0) AS total_ratings,
            COALESCE(AVG(ubr.rating), 0) AS avg_rating,
            100 AS confidence_threshold,  -- Example constant
            4.5 AS global_avg_rating      -- Example global average rating
        FROM businesses b
        LEFT JOIN user_based_ratings ubr ON b.business_id = ubr.business_id
        LEFT JOIN business_based_ratings bbr ON b.business_id = bbr.business_id
        GROUP BY b.business_id, b.business_name
        -- Calculate Bayesian rating and sort by combined score and Bayesian rating
        ORDER BY (user_based_score + business_based_score) DESC, 
                ((avg_rating * total_ratings) + (confidence_threshold * global_avg_rating)) / 
                (total_ratings + confidence_threshold) DESC
        LIMIT %s;
        """
        cur.execute(query, (user_id, user_id, category, user_id, user_id, limit))
        results = cur.fetchall()
        return results

def print_recommendations(recommendations):
    for idx, rec in enumerate(recommendations):
        print(f"{idx + 1}. {rec['business_name']} ({rec['business_id']})")

if __name__ == "__main__":

    tests = [
        {
            'num_businesses': 10000,
            # 'user_id': "108416619844777498346",  # Difficult user_id to get user-business-based recs on, takes a lot of time
            'user_id': "108987883798305430608",
            'category': "Restaurant"
        }
    ]

    for test in tests:
        num_businesses = test['num_businesses']
        user_id = test['user_id']
        category = test['category']

        conn = get_db_connection(num_businesses)
        engine = MySQLRecommendationEngine(conn)
        limit=5

        try:
            start_time = time.time()
            recommendations = engine.get_recommendations(user_id, category=category, limit=limit)
            end_time = time.time()
            print("Recommendations:")
            print_recommendations(recommendations)
            print(f"Time taken: {end_time - start_time} s.")

            print("--------------------")

            start_time = time.time()
            recommendations_fallback = engine._fetch_fallback_recommendations(category, limit=limit)
            end_time = time.time()
            print("Fallback recommendations:")
            print_recommendations(recommendations_fallback)
            print(f"Time taken: {end_time - start_time} s.")

            print("--------------------")

            start_time = time.time()
            recommendations_user = engine._fetch_recommendations_user(user_id, category, limit=limit)
            end_time = time.time()
            print("User-based recommendations:")
            print_recommendations(recommendations_user)
            print(f"Time taken: {end_time - start_time} s.")

            print("--------------------")

            start_time = time.time()
            recommendations_user_business = engine._fetch_recommendations_user_business(user_id, category, limit=limit)
            end_time = time.time()
            print("User-business-based recommendations:")
            print_recommendations(recommendations_user_business)
            print(f"Time taken: {end_time - start_time} s.")

            print("--------------------")

            start_time = time.time()
            recommendations_bayesian = engine.get_recommendations_bayesian(user_id, category, limit=limit)
            end_time = time.time()
            print("Bayesian recommendations:")
            print_recommendations(recommendations_bayesian)
            print(f"Time taken: {end_time - start_time} s.")

        finally:
            conn.close()
    