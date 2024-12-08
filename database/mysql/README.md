# MySQL Recommender Engine Setup

## Table of Contents
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Troubleshooting](#troubleshooting)

## Prerequisites
- **Python 3.7+**
- **MySQL 8.x or later**
- **MySQL Python connector**:

> **Note:** Following the installation instructions in the main README will automatically install all required dependencies and set up the necessary environment.

---

## Installation

1. **Set up  MySQL database**:
   - Login to MySQL as an admin and create a user with the following command:

        ```
        CREATE USER 'cs6400'@'localhost' IDENTIFIED BY 'qwertyuiop';
        ```
    
    - For the subset you want to experiment with, create a database and assign permissions with the following command (the below command does this for the subset of data with 1000 businesses):

        ```
        CREATE DATABASE IF NOT EXISTS cs6400_1000;
        GRANT ALL PRIVILEGES ON cs6400_1000.* TO 'cs6400'@'localhost';
        ```
---

## Usage

### **1. Load Data into MySQL**

**File:** `loader.py`  
This script is used to load user, business, and category data into Neo4j. It creates constraints, indexes, and relationships between data nodes.

**Usage:**
To load business and rating data into the MySQL database, run the following command:
```bash
python3 database/mysql/loader.py
```

---

### **2. Calculate User and Business Similarities**

**File:** `similarity.py`  
This script calculates the similarities between users and businesses. It performs the following tasks:

- **Set Up Indexes**: Creates indexes on user IDs, business IDs, and relationships to improve query performance.
- **Calculate User Similarity**: Computes cosine similarity for users based on their shared ratings of businesses.
- **Calculate Business Similarity**: Computes Jaccard similarity for businesses based on their shared categories.
- **Log Outputs**: Uses Python's logging to track the progress and errors during the similarity calculation.

To calculate user and business similarities, run:
```bash
python3 database/mysql/similarity.py
```

This script performs the following actions:
1. **User Similarity Calculation**:  
   - Users are compared based on their shared ratings for businesses.  
   - Cosine similarity is used to determine similarity between users.  
   - Relationships are stored in the database in a user similarity table.

2. **Business Similarity Calculation**:  
   - Businesses are compared based on their shared categories.  
   - Jaccard similarity is used to determine similarity between businesses.  
   - Relationships are stored in the database in a business similarity tables. 

---

## Troubleshooting

1. **Database Not Running**
   - Ensure that MySQL is running.

2. **Authentication Issues**
   - Ensure that you are using the correct username and password for MySQL, specified above.
   - Update the connection credentials in relevant files if different from the above credentials.

3. **Out of Memory Issues**
   - Large datasets may consume a lot of memory. Consider reducing the batch sizes for data loading.
