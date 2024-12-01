-- Drop tables if they exist, avoid duplication
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS businesses;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS business_categories;
DROP TABLE IF EXISTS user_similarity;
DROP TABLE IF EXISTS business_similarity;
SET FOREIGN_KEY_CHECKS = 1;

-- Table to store business data
CREATE TABLE businesses (
    business_id VARCHAR(50) PRIMARY KEY,
    business_name VARCHAR(255),
    avg_rating DECIMAL(5, 2),
    num_reviews INT
);

-- Table to store user data
CREATE TABLE users (
    user_id VARCHAR(25) PRIMARY KEY
);
-- No other fields available: in a more detailed application,
-- they would likely have more detailed user data.

-- Table to store rating data
CREATE TABLE ratings (
    rating_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    business_id VARCHAR(50),
    user_id VARCHAR(25),
    rating TINYINT,
    timestamp DATETIME,
    FOREIGN KEY (business_id) REFERENCES businesses(business_id),
    FOREIGN KEY (user_id) REFERENCES users (user_id)
);

-- Table to store business-categories (no primary key!)
CREATE TABLE business_categories (
    business_id VARCHAR (50),
    category_name VARCHAR(50),
    PRIMARY KEY(business_id, category_name),
    FOREIGN KEY (business_id) REFERENCES businesses(business_id)
);

-- Table to store user similarities
CREATE TABLE user_similarity (
    user_id_1 VARCHAR(50) NOT NULL, -- User 1 ID, must not be NULL
    user_id_2 VARCHAR(50) NOT NULL, -- User 2 ID, must not be NULL
    similarity_score DECIMAL(5, 4) NOT NULL, -- Cosine similarity score
    common_rated_items INT NOT NULL, -- Number of common rated items
    last_updated BIGINT NOT NULL, -- Timestamp of the last update
    PRIMARY KEY (user_id_1, user_id_2), -- Composite primary key
	INDEX idx_user_id_1 (user_id_1), -- Index for fast lookups on user_id_1
	INDEX idx_user_id_2 (user_id_2) -- Index for fast lookups on user_id_2
);

-- Table to store business similarities
CREATE TABLE business_similarity (
    business_id_1 VARCHAR(50) NOT NULL, -- First business ID, must not be NULL
    business_id_2 VARCHAR(50) NOT NULL, -- Second business ID, must not be NULL
    similarity_score DECIMAL(5, 4) NOT NULL DEFAULT 0.0000, -- Similarity score with default value
    common_categories INT NOT NULL DEFAULT 0, -- Number of common categories, default to 0
    last_updated BIGINT NOT NULL, -- Timestamp for the last update
    PRIMARY KEY (business_id_1, business_id_2), -- Composite primary key
    INDEX idx_business_id_1 (business_id_1), -- Index for business_id_1
    INDEX idx_business_id_2 (business_id_2)  -- Index for business_id_2
);

-- Indexes to optimize frequent lookups by business and user
-- CREATE INDEX idx_business_id on reviews (business_id);