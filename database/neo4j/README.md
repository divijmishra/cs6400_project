# Neo4j Recommender Engine Setup

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites
- **Python 3.7+**
- **Neo4j 4.x or later** installed and running locally or remotely
- **Neo4j Python Driver**: `pip install neo4j`
- **Pandas**: `pip install pandas`
- **APOC Procedures** (for some advanced Cypher queries): [APOC Plugin Installation Guide](https://neo4j.com/docs/apoc/current/installation/)

> **Note:** Following the installation instructions in the main README will automatically install all required dependencies and set up the necessary environment.

---

## Installation

1. **Set up Neo4j database**:
   - Start Neo4j (Neo4jDesktop) and create a new database with following credentials (code expects: `neo4j/qwertyuiop`).
   - Start the new database.

---

## Usage

### **1. Load Data into Neo4j**

**File:** `load_data.py`  
This script is used to load user, business, and category data into Neo4j. It creates constraints, indexes, and relationships between data nodes. The key functions are:

- **create_constraints()**: Creates unique constraints and indexes for User, Business, and Category nodes.
- **create_schema_for_recommendations()**: Sets up composite indexes and constraints for recommendation relationships.
- **load_businesses_json()**: Loads business data from a JSON file.
- **load_businesses_csv()**: Loads business data from a CSV file.
- **load_ratings()**: Loads user ratings from a CSV file.
- **clear_database()**: Clears the Neo4j database, including constraints, indexes, nodes, and relationships.


**Usage:**
To load business and rating data into the Neo4j database, run the following command:
```bash
python load_data.py --clear --schema --load --ratings path/to/ratings.csv --metadata path/to/metadata.json
```

**Options:**
- `--clear`: Clears the database (removes all nodes, relationships, indexes, and constraints)
- `--schema`: Sets up the database schema (constraints and indexes)
- `--load`: Loads data from the provided ratings and metadata files
- `--ratings`: Path to the CSV file containing user ratings
- `--metadata`: Path to the JSON file containing business metadata

**Example Usage**:
```bash
python load_data.py --clear --schema --load --ratings data/load/filtered_ratings_10k.csv --metadata data/load/matched_business_10k.csv
```

---

### **2. Calculate User and Business Similarities**

**File:** `similarity_calculator_no_cache.py`  
This script calculates the similarities between users and businesses. It performs the following tasks:

- **Set Up Indexes**: Creates indexes on user IDs, business IDs, and relationships to improve query performance.
- **Calculate User Similarity**: Computes cosine similarity for users based on their shared ratings of businesses.
- **Calculate Business Similarity**: Computes Jaccard similarity for businesses based on their shared categories.
- **Log Outputs**: Uses Python's logging to track the progress and errors during the similarity calculation.

To calculate user and business similarities, run:
```bash
python similarity_calculator_no_cache.py
```

This script performs the following actions:
1. **User Similarity Calculation**:  
   - Users are compared based on their shared ratings for businesses.  
   - Cosine similarity is used to determine similarity between users.  
   - Relationships are stored in the database as `SIMILAR_TO` relationships between User nodes.  

2. **Business Similarity Calculation**:  
   - Businesses are compared based on their shared categories.  
   - Jaccard similarity is used to determine similarity between businesses.  
   - Relationships are stored in the database as `SIMILAR_TO` relationships between Business nodes.  

**Key Classes & Functions:**
- **class SimilarityCalculatorNoCache**: Handles all similarity calculations and updates.
- **calculate_user_similarity()**: Calculates similarities between users.
- **calculate_business_similarity()**: Calculates similarities between businesses.
- **update_user_similarity(affected_users)**: Updates similarity scores for a list of affected users.  

---

## Troubleshooting

1. **Database Not Running**
   - Ensure that Neo4j is running.

2. **Authentication Issues**
   - Ensure that you are using the correct username and password for Neo4j.
   - Update the connection credentials in relevant files if different from default values (`neo4j/qwertyuiop`)..

3. **Out of Memory Issues**
   - Large datasets may consume a lot of memory. Consider reducing the batch sizes for data loading.

4. **Deadlocks**
   - The `similarity_calculator_no_cache.py` script includes retry logic to handle deadlocks, but if they persist, reduce the batch size.
