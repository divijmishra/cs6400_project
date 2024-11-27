-- Drop tables if they exist, avoid duplication
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS businesses;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS business_categories;
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

-- Indexes to optimize frequent lookups by business and user
-- CREATE INDEX idx_business_id on reviews (business_id);