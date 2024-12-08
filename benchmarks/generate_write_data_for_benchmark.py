import pandas as pd
import random
import os

random.seed(12345)

# Load the data
filtered_1k_ratings = pd.read_csv("data/samples/filtered_ratings_1k.csv")
filtered_1k_businesses = pd.read_csv("data/samples/matched_businesses_1k.csv")
# filtered_1k_ratings = pd.read_csv("data/samples/filtered_ratings_5k.csv")
# filtered_1k_businesses = pd.read_csv("data/samples/matched_businesses_5k.csv")
# filtered_1k_ratings = pd.read_csv("data/samples/filtered_ratings_10k.csv")
# filtered_1k_businesses = pd.read_csv("data/samples/matched_businesses_10k.csv")
# Datasets to process: (businesses, ratings)
business_sets = [
    ('1k', filtered_1k_businesses, filtered_1k_ratings),
    # ('5k', filtered_5k_businesses, filtered_5k_ratings),
    # ('10k', filtered_10k_businesses, filtered_10k_ratings)
]

def generate_ratings(businesses, ratings, size):
    valid_businesses = list(businesses)
    valid_users = list(ratings['user'].unique())
    
    # Existing business-user pairs from filtered ratings
    existing_pairs = set(zip(ratings['business'], ratings['user']))
    
    new_pairs = []
    
    while len(new_pairs) < size:
        # Randomly select a business and a user
        business = random.choice(valid_businesses)
        user = random.choice(valid_users)
        
        if (business, user) not in existing_pairs:
            dummy_rating = random.randint(1, 5)  # Random rating between 1 and 5
            timestamp = 1539819804101  # Placeholder timestamp
            new_pairs.append((business, user, dummy_rating, timestamp))
            existing_pairs.add((business, user))
    
    dummy_df = pd.DataFrame(new_pairs, columns=['business', 'user', 'rating', 'timestamp'])
    
    return dummy_df

dummy_ratings_dict = {}

# For each business dataset, create dummy ratings of size 9000
for label, filtered_businesses, filtered_ratings in business_sets:
    dummy_ratings_9000 = generate_ratings(filtered_businesses['gmap_id'], filtered_ratings, 9000)
    dummy_ratings_dict[f"{label}_9000"] = dummy_ratings_9000

# Save the dummy datasets to new CSVs
for key, df in dummy_ratings_dict.items():
    file_path = f"data/benchmark/{key}_dummy_ratings.csv"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(f"data/benchmark/{key}_dummy_ratings.csv", index=False)