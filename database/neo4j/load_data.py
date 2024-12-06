from neo4j import GraphDatabase
import pandas as pd
import json
import argparse
import ast
from neo4j_connection import Neo4jConnection

def create_constraints(conn):
    constraints = [
        """CREATE CONSTRAINT user_id IF NOT EXISTS 
           FOR (u:User) REQUIRE u.user_id IS UNIQUE""",
        """CREATE CONSTRAINT business_id IF NOT EXISTS 
           FOR (b:Business) REQUIRE b.gmap_id IS UNIQUE""",
        """CREATE CONSTRAINT category_name IF NOT EXISTS 
           FOR (c:Category) REQUIRE c.name IS UNIQUE""",
           
        """CREATE CONSTRAINT business_name_exists IF NOT EXISTS 
           FOR (b:Business) REQUIRE b.name IS NOT NULL""",
        """CREATE CONSTRAINT business_avg_rating_exists IF NOT EXISTS 
           FOR (b:Business) REQUIRE b.avg_rating IS NOT NULL""",
        """CREATE CONSTRAINT business_num_reviews_exists IF NOT EXISTS 
           FOR (b:Business) REQUIRE b.num_of_reviews IS NOT NULL"""
    ]
    
    indexes = [
        """CREATE INDEX rating_idx IF NOT EXISTS 
           FOR ()-[r:RATED]-() ON (r.rating)""",
        """CREATE INDEX timestamp_idx IF NOT EXISTS 
           FOR ()-[r:RATED]-() ON (r.timestamp)""",
           
        """CREATE INDEX business_avg_rating_idx IF NOT EXISTS 
           FOR (b:Business) ON (b.avg_rating)""",
        """CREATE INDEX business_category_idx IF NOT EXISTS 
           FOR ()-[r:BELONGS_TO]-() ON (r.category)""",
        """CREATE INDEX similarity_score_idx IF NOT EXISTS 
           FOR ()-[s:SIMILAR_TO]-() ON (s.score)""",
        """CREATE INDEX similarity_timestamp_idx IF NOT EXISTS 
           FOR ()-[s:SIMILAR_TO]-() ON (s.last_updated)"""
    ]
    
    for constraint in constraints:
        try:
            conn.query(constraint)
        except Exception as e:
            print(f"Warning: Could not create constraint: {str(e)}")
            
    for index in indexes:
        try:
            conn.query(index)
        except Exception as e:
            print(f"Warning: Could not create index: {str(e)}")

def create_schema_for_recommendations(conn):
    composite_indexes = [
        """CREATE INDEX user_rating_composite_idx IF NOT EXISTS 
           FOR ()-[r:RATED]-() ON (r.rating, r.timestamp)""",
        """CREATE INDEX similar_business_composite_idx IF NOT EXISTS 
           FOR ()-[s:SIMILAR_TO]-() ON (s.score, s.last_updated)"""
    ]
    
    relationship_constraints = [
        """CREATE CONSTRAINT similar_to_score_exists IF NOT EXISTS 
           FOR ()-[s:SIMILAR_TO]-() REQUIRE s.score IS NOT NULL""",
        """CREATE CONSTRAINT similar_to_timestamp_exists IF NOT EXISTS 
           FOR ()-[s:SIMILAR_TO]-() REQUIRE s.last_updated IS NOT NULL"""
    ]
    
    for index in composite_indexes:
        try:
            conn.query(index)
        except Exception as e:
            print(f"Warning: Could not create composite index: {str(e)}")
            
    for constraint in relationship_constraints:
        try:
            conn.query(constraint)
        except Exception as e:
            print(f"Warning: Could not create relationship constraint: {str(e)}")

def load_businesses_json(conn, metadata_file, batch_size=1000, max_entries=1000):
    print("Loading business data from JSON file...")
    def process_batch(batch):
        query = """
        UNWIND $batch AS business
        MERGE (b:Business {gmap_id: business.gmap_id})
        SET b += {
            name: business.name,
            avg_rating: COALESCE(business.avg_rating, 0.0),
            num_of_reviews: COALESCE(business.num_of_reviews, 0),
            price: business.price,
            latitude: business.latitude,
            longitude: business.longitude
        }
        WITH b, business
        UNWIND business.category AS category
        MERGE (c:Category {name: category})
        MERGE (b)-[:BELONGS_TO {
            weight: 1.0,
            last_updated: timestamp()
        }]->(c)
        """
        conn.query(query, {'batch': batch})
    
    if(max_entries == -1):
        max_entries = float('inf')
    
    current_batch = []
    total_entries = 0
    
    with open(metadata_file) as file:
        for line in file:
            if total_entries >= max_entries:
                break
            business = json.loads(line.strip())
            current_batch.append({
                'gmap_id': business['gmap_id'],
                'name': business['name'],
                'category': business['category'],
                'avg_rating': business.get('avg_rating', 0.0),
                'num_of_reviews': business.get('num_of_reviews', 0)
            })
            total_entries += 1
            
            if len(current_batch) >= batch_size:
                process_batch(current_batch)
                current_batch = []
    
    # Process remaining batch
    if current_batch:
        process_batch(current_batch)
        
def load_businesses_csv(conn, metadata_file, batch_size=1000, max_entries=1000):
    print("Loading businesses from CSV file...")
    def process_batch(batch):
        query = """
        UNWIND $batch AS business
        MERGE (b:Business {gmap_id: business.gmap_id})
        SET b += {
            name: business.name,
            avg_rating: COALESCE(business.avg_rating, 0.0),
            num_of_reviews: COALESCE(business.num_of_reviews, 0),
            price: business.price,
            latitude: business.latitude,
            longitude: business.longitude
        }
        WITH b, business
        UNWIND business.categories AS category
        WITH b, category, business.categories AS categories
        WHERE category IS NOT NULL AND category <> ''
        MERGE (c:Category {name: category})
        MERGE (b)-[:BELONGS_TO {
            weight: 1.0 / size(categories),
            last_updated: timestamp()
        }]->(c)
        """
        conn.query(query, {'batch': batch})
    
    if(max_entries == -1):
        max_entries = float('inf')

    current_batch = []
    total_entries = 0

    for chunk in pd.read_csv(metadata_file, chunksize=batch_size):
        for _, row in chunk.iterrows():
            if total_entries >= max_entries:
                break
            
            try:
                categories = ast.literal_eval(row['category']) if pd.notna(row['category']) else []
            except:
                categories = []
            
            business = {
                'gmap_id': row['gmap_id'],
                'name': row['name'],
                'categories': categories,
                'avg_rating': row.get('avg_rating', 0.0),
                'num_of_reviews': row.get('num_of_reviews', 0),
                'price': row.get('price', ''),
                'latitude': row.get('latitude', 0.0),
                'longitude': row.get('longitude', 0.0)
            }
            current_batch.append(business)
            total_entries += 1

            if len(current_batch) >= batch_size:
                process_batch(current_batch)
                current_batch = []

        if total_entries >= max_entries:
            break

    # Process remaining batch
    if current_batch:
        process_batch(current_batch)

def load_ratings(conn, ratings_file, batch_size=10000, max_entries=1000):
    print("Loading ratings data from CSV file...")
    def process_batch(batch):
        query = """
        UNWIND $batch AS rating
        MERGE (u:User {user_id: rating.user})
        WITH u, rating
        MATCH (b:Business {gmap_id: rating.business})
        MERGE (u)-[r:RATED]->(b)
        SET r.rating = rating.rating,
            r.timestamp = rating.timestamp,
            r.last_updated = timestamp(),
            r.normalized_rating = CASE 
                WHEN rating.rating >= 4.5 THEN 5
                WHEN rating.rating >= 3.5 THEN 4
                WHEN rating.rating >= 2.5 THEN 3
                WHEN rating.rating >= 1.5 THEN 2
                ELSE 1
            END
        """
        conn.query(query, {'batch': batch})

    if(max_entries == -1):
        max_entries = float('inf')

    current_batch = []
    total_entries = 0
    
    for chunk in pd.read_csv(ratings_file, chunksize=batch_size):
        for record in chunk.to_dict('records'):
            if total_entries >= max_entries:
                break
            current_batch.append(record)
            total_entries += 1
            
            if len(current_batch) >= batch_size:
                process_batch(current_batch)
                current_batch = []
        
        if total_entries >= max_entries:
            break
    
    # Process remaining batch
    if current_batch:
        process_batch(current_batch)

def clear_database(conn):
    """
    Clear all nodes, relationships, indexes, and constraints from the database
    """
    # Drop all constraints and indexes first
    queries = [
        # Drop all constraints
        "SHOW CONSTRAINTS",
        "DROP CONSTRAINT",
        
        # Drop all indexes
        "SHOW INDEXES",
        "DROP INDEX",

        # Delete all relationships
        "CALL apoc.periodic.iterate("
        "  'MATCH ()-[r]->() RETURN r',"
        "  'DELETE r',"
        "  {batchSize: 1000, iterateList: true}"
        ") YIELD batches, total"
        " RETURN batches, total",
        
        # Delete all nodes
        "CALL apoc.periodic.iterate("
        "  'MATCH (n) RETURN n',"
        "  'DETACH DELETE n',"
        "  {batchSize: 10000, parallel: false}"
        ") YIELD batches, total"
        " RETURN batches, total"
    ]
    
    constraints = conn.query(queries[0])
    for constraint in constraints:
        if 'name' in constraint: 
            conn.query(f"{queries[1]} {constraint['name']}")
        else:
            conn.query(f"{queries[1]} {constraint['constraintName']}")
    
    indexes = conn.query(queries[2])
    for index in indexes:
        if 'name' in index:
            conn.query(f"{queries[3]} {index['name']}")
        else:
            conn.query(f"{queries[3]} {index['indexName']}")
    
    conn.query(queries[4])

    conn.query(queries[5])
    
    print("Database cleared successfully")

def main(clear_existing=False, setup_schema=False, load_data=False, 
         ratings_file='data/rating-Georgia.csv', metadata_file='data/meta-Georgia.json'):
    """
    Main function to set up Neo4j database
    
    Parameters:
    clear_existing (bool): If True, clears all existing data before loading
    setup_schema (bool): If True, sets up the schema constraints and indexes
    load_data (bool): If True, loads the business and ratings data
    ratings_file (str): Path to the ratings CSV file
    metadata_file (str): Path to the metadata JSON file
    """
    # Connect to Neo4j
    conn = Neo4jConnection(
        uri="neo4j://localhost:7687",
        user="neo4j",
        password="qwertyuiop"
    )

    try:
        # Clear database if requested
        if clear_existing:
            print("Clearing existing database...")
            clear_database(conn)

        # Set up schema if requested
        if setup_schema:
            print("Setting up schema...")
            create_constraints(conn)
            print("Schema constraints created successfully")
            
            create_schema_for_recommendations(conn)
            print("Recommendation schema created successfully")

        # Load data if requested
        if load_data:
            print("Loading data...")
            try:
                # Check file extension and call the appropriate function
                if metadata_file.endswith('.json'):
                    load_businesses_json(conn, metadata_file, max_entries=-1)
                elif metadata_file.endswith('.csv'):
                    load_businesses_csv(conn, metadata_file, max_entries=-1)
                else:
                    print("Unsupported file format for business data. Please provide a JSON or CSV file.")
                print("Business data loaded successfully")
            except Exception as e:
                print(f"Error loading business data: {str(e)}")
                raise

            try:
                load_ratings(conn, ratings_file, max_entries=-1)
                print("Ratings data loaded successfully")
            except Exception as e:
                print(f"Error loading ratings data: {str(e)}")
                raise

            print("Data loading completed successfully")
        
        if not any([clear_existing, setup_schema, load_data]):
            print("No operations requested. Use --help to see available options.")
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Set up and load data into Neo4j database')
    parser.add_argument('--clear', action='store_true', 
                      help='Clear existing database before any operations')
    parser.add_argument('--schema', action='store_true',
                      help='Set up database schema (constraints and indexes)')
    parser.add_argument('--load', action='store_true',
                      help='Load data into the database')
    parser.add_argument('--ratings', type=str, default='data/rating-Georgia.csv',
                      help='Path to ratings CSV file')
    parser.add_argument('--metadata', type=str, default='data/meta-Georgia.json',
                      help='Path to metadata JSON file')
    
    args = parser.parse_args()
    
    try:
        main(clear_existing=args.clear,
             setup_schema=args.schema,
             load_data=args.load,
             ratings_file=args.ratings,
             metadata_file=args.metadata)
    except Exception as e:
        print(f"Script failed: {str(e)}")
        exit(1)