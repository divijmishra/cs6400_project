from database.neo4j.neo4j_connection import Neo4jConnection
import logging
import time

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CollaborativeRecommendationEngine:
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
        query = """
        MATCH (u:User {user_id: $user_id})-[:RATED]->(b1:Business)
        WITH u, COLLECT(DISTINCT b1) AS userRatedBusinesses

        MATCH (other:User)-[:RATED]->(b1)
        WHERE b1 IN userRatedBusinesses AND other <> u
        WITH u, userRatedBusinesses, COLLECT(DISTINCT other) AS similarUsers

        UNWIND similarUsers AS similarUser
        MATCH (similarUser)-[r:RATED]->(b2:Business)-[:BELONGS_TO]->(c:Category {name: $category})
        WHERE NOT b2 IN userRatedBusinesses
        WITH b2, COUNT(DISTINCT r) AS score
        RETURN b2.name AS business_name, b2.gmap_id AS business_id, score
        ORDER BY score DESC
        LIMIT $limit
        """

        with self.conn.driver.session() as session:
            recommendations = session.run(query, {
                'user_id': user_id,
                'category': category,
                'limit': limit
            })
            return [record.data() for record in recommendations]

    def _fetch_fallback_recommendations(self, category, limit):
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

        with self.conn.driver.session() as session:
            fallback_recommendations = session.run(query, {
                'category': category,
                'limit': limit
            })
            return [record.data() for record in fallback_recommendations]
    
    def _fetch_recommendations_user(self, user_id, category, limit):
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

        with self.conn.driver.session() as session:
            recommendations = session.run(query, {'user_id': user_id, 'category': category, 'limit': limit})
            return [record.data() for record in recommendations]

    def _fetch_recommendations_user_business(self, user_id, category, limit):
        """
        Fetch recommendations based on user-user and business-business SIMILAR_TO relationships.
        """
        query = """
        // Get businesses rated by the target user
        MATCH (u:User {user_id: $user_id})-[r_user:RATED]->(b_rated:Business)
        WITH u, 
            COLLECT(DISTINCT b_rated) AS userRatedBusinesses, 
            COLLECT({business: b_rated, rating: r_user.rating}) AS userRatedBusinessRatings

        // Get similar users and their similarity scores
        CALL (u, userRatedBusinesses) {
            MATCH (u)-[s1:SIMILAR_TO]-(similar:User)
            MATCH (similar)-[r:RATED]->(b:Business)-[:BELONGS_TO]->(:Category {name: $category})
            WHERE NOT b IN userRatedBusinesses
            RETURN b.gmap_id AS business_id, b, SUM(r.rating * s1.score) AS user_based_score
        }

        // Collect user-based scores
        WITH COLLECT({
                business_id: business_id, 
                business: b, 
                user_based_score: user_based_score
            }) AS userScores, 
            userRatedBusinesses, 
            userRatedBusinessRatings, 
            u

        // Compute business-based scores
        CALL (userRatedBusinesses, userRatedBusinessRatings) {
            UNWIND userRatedBusinessRatings AS urb
            WITH urb.business AS ratedBusiness, urb.rating AS rating, userRatedBusinesses
            MATCH (ratedBusiness)-[s2:SIMILAR_TO]->(b:Business)
            WHERE NOT b IN userRatedBusinesses
            RETURN b.gmap_id AS business_id, b, SUM(rating * s2.score) AS business_based_score
        }

        // Collect business-based scores
        WITH userScores, 
            COLLECT({
                business_id: business_id, 
                business: b, 
                business_based_score: business_based_score
            }) AS businessScores

        // Combine the scores
        WITH userScores + businessScores AS combinedScores
        UNWIND combinedScores AS cs
        WITH cs.business_id AS business_id, 
            cs.business AS b,
            COALESCE(cs.user_based_score, 0) AS user_based_score,
            COALESCE(cs.business_based_score, 0) AS business_based_score

        // Aggregate per business_id
        WITH business_id, b,
            SUM(user_based_score) AS total_user_based_score,
            SUM(business_based_score) AS total_business_based_score

        // Fetch additional business details and compute total score
        RETURN 
            b.name AS business_name, 
            business_id,
            total_user_based_score, 
            total_business_based_score,
            b.num_reviews AS total_ratings,
            b.avg_rating,
            (total_user_based_score + total_business_based_score) AS total_score
        ORDER BY total_score DESC, b.avg_rating DESC
        LIMIT $limit
        """

        with self.conn.driver.session() as session:
            recommendations = session.run(query, {'user_id': user_id, 'category': category, 'limit': limit})
            return [record.data() for record in recommendations]

def print_recommendations(recommendations):
    for idx, rec in enumerate(recommendations):
        print(f"{idx + 1}. {rec['business_name']} ({rec['business_id']})")        

def main():
    conn = Neo4jConnection(
        uri="neo4j://localhost:7687",
        user="neo4j",
        password="qwertyuiop"
    )

    engine = CollaborativeRecommendationEngine(conn)
    try:
        # user_id = "107065929445511534118" 
        user_id = "108416619844777498346" 
        # user_id = "108987883798305430608"  # Example user ID
        category = "Restaurant"  # Example category selected by the user

        start_time = time.time()
        recommendations = engine.get_recommendations(user_id, category=category, limit=5)
        end_time = time.time()
        print("Recommendations:")
        for rec in recommendations:
            print(rec)
        # print_recommendations(recommendations)
        print(f"Time taken: {end_time - start_time} s.")

        print("--------------------")
        start_time = time.time()
        recommendations_fallback = engine._fetch_fallback_recommendations(category, limit=5)
        end_time = time.time()
        # print("Fallback recommendations:", recommendations_fallback)
        print("Fallback recommendations:")
        for rec in recommendations_fallback:
            print(rec)
        # print_recommendations(recommendations_fallback)
        print(f"Time taken: {end_time - start_time} s.")

        print("--------------------")

        start_time = time.time()
        recommendations_user = engine._fetch_recommendations_user(user_id, category, limit=5)
        end_time = time.time()
        # print("User-based recommendations:", recommendations_user)
        print("User-based recommendations:")
        for rec in recommendations_user:
            print(rec)
        # print_recommendations(recommendations_user)
        print(f"Time taken: {end_time - start_time} s.")

        print("--------------------")

        start_time = time.time()
        recommendations_user_business = engine._fetch_recommendations_user_business(user_id, category, limit=5)
        end_time = time.time()
        # print("User-business-based recommendations:", recommendations_user_business)
        print("User-business-based recommendations:")
        for rec in recommendations_user_business:
            print(rec)
        # print_recommendations(recommendations_user_business)
        print(f"Time taken: {end_time - start_time} s.")
        print("--------------------")
    finally:
        conn.close()

if __name__ == "__main__":
    main()