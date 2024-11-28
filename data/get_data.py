"""
Downloads data, extracts files, and creates subsets for experimentation.
Data source: https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/googlelocal/

We're working with the Georgia dataset, which contains 24M reviews and 166k businesses. This code creates 4 subsets:
> extra small (100 businesses => ~23k ratings)
> small (1k businesses => ~170k ratings)
> medium (10k businesses => ~1.4M ratings)
> large (100k businesses => ~14M ratings)
(Note the exact subsets and number of ratings are seed-dependent)

Data downloaded to data/raw/, subsets saved to data/samples/.

(Took ~20 min to run on my local system. Filtering businesses for 100k takes ~half of that time.)
"""

import os
import json
import gzip
import shutil
import requests
import pandas as pd

# Choose a subset of N businesses, N users, and the corresponding reviews
# such that we choose businesses and users with lots of reviews
def save_core_subset_of_ratings(ratings_file, metadata_file, output_dir, num_businesses=100):
    """
    Loads raw ratings data and business metadata.
    Selects the top (num_businesses) businesses according to how many reviews
    they have.
    Filters ratings for ratings of the selected businesses.
    Among the users left, selects the top (num_businesses) users according to 
    how many businesses they've reviewed in this subset.
    Filters ratings for these users again, resulting in a dataset with
    (num_businesses) businesses, (num_businesses) users, and lots of reviews. 

    Arguments
        ratings_file : path to raw ratings.csv 
        metadata_file: path to raw metadata.csv
        output_dir   : directory to save ratings of the chosen subset of businesses

    Returns: 
        None
    """

    try:
        ratings_df = pd.read_csv(ratings_file)
        # print(f"Loaded data from {ratings_file} with {len(ratings_df)} rows.")
    except FileNotFoundError:
        print(f"Error: The file {ratings_file} was not found.")
        return
    
    try:
        metadata_df = pd.read_json(metadata_file, lines=True)
        # print(f"Loaded data from {metadata_file} with {len(metadata_df)} rows.")
    except FileNotFoundError:
        print(f"Error: The file {metadata_file} was not found.")
        return

    if 'business' not in ratings_df.columns:
        print("Error: 'business' column is not present in the CSV file.")
        return

    # Select the top {num_businesses} businesses acc to number of reviews
    top_businesses = metadata_df.nlargest(num_businesses, 'num_of_reviews')

    # Filter ratings once
    filtered_ratings = ratings_df[ratings_df['business'].isin(top_businesses['gmap_id'])]

    # Select the top {num_businesses} users in the filtered  set acc to number of reviews
    user_activity = (
        filtered_ratings.groupby('user')
            .size()
            .reset_index(name='num_ratings')
            .sort_values(by='num_ratings', ascending=False)
    )
    top_users = user_activity.head(num_businesses)['user']

    # Filter ratings again
    final_ratings = filtered_ratings[filtered_ratings['user'].isin(top_users)]

    # Write statistics to a txt file
    with open(f"{output_dir}/stats_{num_businesses}.txt", "w") as file:
        file.write(f"Sample contains {num_businesses} businesses, {len(final_ratings)} ratings, {num_businesses} users.")

    # Save metadata subset
    metadata_output_file = f"{output_dir}/metadata_{num_businesses}.csv"
    metadata_output_df = pd.json_normalize(top_businesses)
    metadata_output_df.to_csv(metadata_output_file, index=False, encoding='utf-8')
    print(f"Filtered business information saved to '{metadata_output_file}'.")

    # Save ratings subset
    ratings_output_file = f"{output_dir}/ratings_{num_businesses}.csv" 
    final_ratings.to_csv(ratings_output_file, index=False)
    print(f"Filtered data saved to '{ratings_output_file}'.")

# Creates a directory if it doesn't already exist
def create_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Directory '{directory} created.")
    else:
        print(f"Directory '{directory}' already exists.")

# Function to download a file from a URL
def download_file(url, dest_path):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest_path, 'wb') as file:
            shutil.copyfileobj(response.raw, file)
        print(f"Downloaded: {dest_path}")
    else:
        print(f"Failed to download {url}, status code: {response.status_code}")

# Function to extract a .gz file
def extract_gz(file_path, extract_to):
    with gzip.open(file_path, 'rb') as gz_file:
        with open(extract_to, 'wb') as extracted_file:
            shutil.copyfileobj(gz_file, extracted_file)
            print(f"Extracted: {file_path} to {extract_to}")

# Function to download Google Local Georgia dataset
def fetch_raw_google_reviews_data():
    # URLs
    ratings_url = "https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/googlelocal/rating-Georgia.csv.gz"
    metadata_url = "https://datarepo.eng.ucsd.edu/mcauley_group/gdrive/googlelocal/meta-Georgia.json.gz"

    # Directory to save and extract raw data
    data_dir = "data/raw"
    create_directory(data_dir)

    # File paths for downloads
    ratings_gz_path = os.path.join(data_dir, "ratings_full.csv.gz")
    metadata_gz_path = os.path.join(data_dir, "metadata_full.json.gz")

    # File paths for extracted files
    ratings_path = os.path.join(data_dir, "ratings_full.csv")
    metadata_path = os.path.join(data_dir, "metadata_full.json")

    # Download the files
    if os.path.exists(ratings_gz_path):
        print(f"{ratings_gz_path} already exists, skipping download.")
    else:
        download_file(ratings_url, ratings_gz_path)
    if os.path.exists(metadata_gz_path):
        print(f"{metadata_gz_path} already exists, skipping download.")
    else: 
        download_file(metadata_url, metadata_gz_path)

    # Extract files
    if os.path.exists(ratings_path):
        print(f"{ratings_path} already exists, skipping extraction.")
    else:
        extract_gz(ratings_gz_path, ratings_path)
    if os.path.exists(metadata_path):
        print(f"{metadata_path} already exists, skipping extraction.")
    else:
        extract_gz(metadata_gz_path, metadata_path)

# Function to create and save subsets of data for experimentation
def save_all_subsets():
    subset_list = [100, 300, 1000, 3000, 10000]

    # Directory to save subsets
    raw_data_dir = "data/raw"
    data_dir = "data/samples"
    create_directory(data_dir)

    # File paths
    raw_ratings_path = os.path.join(raw_data_dir, "ratings_full.csv")
    raw_metadata_path = os.path.join(raw_data_dir, "metadata_full.json")

    # Save subsets
    for num_businesses in subset_list:
        print(f"Creating subset with {num_businesses} businesses")
        save_core_subset_of_ratings(raw_ratings_path, raw_metadata_path, data_dir, num_businesses)

if __name__ == "__main__":
    fetch_raw_google_reviews_data()
    save_all_subsets()

