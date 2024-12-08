# Recommendation Engines

This directory contains the core scripts for generating personalized recommendations from **Neo4j** and **MySQL** databases. Each engine provides collaborative filtering and fallback logic to generate high-quality recommendations for users.

---

## **Prerequisites**
- **Python 3.7+**
- **Neo4j 4.x or later** for `collaborative_recommendation_engine.py`
- **MySQL 8.x or later** for `recommender.py`
- **Neo4j Python Driver**: `pip install neo4j`
- **MySQL Connector**: `pip install mysql-connector-python`

> **Note:** Following the installation instructions in the main README will automatically install all required dependencies and set up the necessary environment.

---

## **File Descriptions**

### 1. **collaborative_recommendation_engine.py**
This script uses **Neo4j** to generate collaborative filtering recommendations. It computes user-based and business-based similarities to recommend businesses to users.

#### **Key Features**
- **Collaborative Recommendations**: Recommends businesses that similar users have rated.
- **Fallback Recommendations**: Provides general recommendations when collaborative filtering fails.
- **User-Business Relationship Analysis**: Utilizes user-business and business-business similarity to improve recommendations.

#### **Usage**
```python
python collaborative_recommendation_engine.py
```

The script generates recommendations for a given `user_id` and `category`. Example usage is defined in the script's `main()` function.

#### **How it Works**
1. **Collaborative Recommendations**: Uses Neo4j to find similar users and recommends businesses they have rated.
2. **Fallback Recommendations**: If no similar users exist, it recommends the highest-rated businesses from the same category.
3. **User-Business Analysis**: Considers relationships between users and businesses as well as relationships between businesses.

#### **Functions**
- **get_recommendations(user_id, category, limit=10)**: Main method to get recommendations for a user.
- **_fetch_recommendations()**: Finds recommendations from users with similar tastes.
- **_fetch_fallback_recommendations()**: Provides category-based fallback recommendations.
- **_fetch_recommendations_user()**: Considers similar users and their rated businesses.
- **_fetch_recommendations_user_business()**: Considers user-business and business-business similarities to provide hybrid recommendations.

---

### 2. **recommender.py**
This script uses **MySQL** to generate collaborative filtering recommendations. It extracts user and business similarity information directly from a MySQL relational database.

#### **Key Features**
- **Collaborative Recommendations**: Recommends businesses that similar users have rated.
- **Fallback Recommendations**: Provides general recommendations when collaborative filtering fails.
- **Hybrid Recommendations**: Combines user-based and business-based similarity to recommend businesses.

#### **Usage**
```python
python recommender.py
```

The script generates recommendations for a given `user_id` and `category`. Example usage is defined in the script's `main()` function.

#### **How it Works**
1. **Collaborative Recommendations**: Uses MySQL to identify similar users and recommends businesses they have rated.
2. **Fallback Recommendations**: If no similar users exist, it recommends the highest-rated businesses from the same category.
3. **User-Business Analysis**: Considers relationships between users and businesses as well as relationships between businesses.

#### **Functions**
- **get_recommendations(user_id, category, limit=10)**: Main method to get recommendations for a user.
- **_fetch_recommendations()**: Finds recommendations from users with similar tastes.
- **_fetch_fallback_recommendations()**: Provides category-based fallback recommendations.
- **_fetch_recommendations_user()**: Considers similar users and their rated businesses.
- **_fetch_recommendations_user_business()**: Considers user-business and business-business similarities to provide hybrid recommendations.

---

## **Usage Instructions**

To generate recommendations, you can run either of the following commands, depending on the desired backend (**Neo4j** or **MySQL**):

1. **Neo4j-based Recommendations**:
   ```bash
   python collaborative_recommendation_engine.py
   ```

2. **MySQL-based Recommendations**:
   ```bash
   python recommender.py
   ```

---

## **Environment Variables and Configuration**

**Neo4j**
- **Host**: Configured in `collaborative_recommendation_engine.py`
- **Username/Password**: Set in `collaborative_recommendation_engine.py` (default: `neo4j/qwertyuiop`).

**MySQL**
- **Host**: Configured in `recommender.py` (default: `localhost`)
- **Username/Password**: Set in `recommender.py` (default: `cs6400/qwertyuiop`).

---

## **Troubleshooting**

**Common Issues**
1. **Authentication Issues**:
   - Ensure Neo4j and MySQL credentials are correct.
   - Update the credentials in `collaborative_recommendation_engine.py` and `recommender.py`.

2. **Database Connection Issues**:
   - Ensure the Neo4j and MySQL servers are running and accessible.
   - Verify the ports, IP addresses, and firewalls are configured correctly.

3. **Missing Packages**:
   - Ensure **Neo4j Python Driver** and **MySQL Connector** are installed.
   - Follow instructions in root **README** to install all dependencies.
