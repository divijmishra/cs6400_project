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

# Randomly choose a subset of businesses from ratings and save their ratings
def save_subset_of_ratings(input_file, output_dir, num_businesses=100000):
    """
    Loads data from csv input_file, randomly selects a subset of 
    num_businesses businesses, and saves their ratings to output_file

    Arguments
        input_file  : path to input data (raw ratings csv)
        output_dir : directory to save ratings of the chosen subset of     
                        businesses

    Returns: 
        output_file: (str) path to saved subset
    """

    try:
        df = pd.read_csv(input_file)
        print(f"Loaded data from {input_file} with {len(df)} rows.")
    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
        return

    if 'business' not in df.columns:
        print("Error: 'business' column is not present in the CSV file.")
        return

    # Randomly select the specified number of unique businesses
    unique_businesses = df['business'].drop_duplicates()
    if len(unique_businesses) < num_businesses:
        print(f"Warning: The dataset contains only {len(unique_businesses)} unique businesses.")
        num_businesses = len(unique_businesses)
    
    sampled_businesses = unique_businesses.sample(n=num_businesses, random_state=42)

    # Filter the dataframe to include only entries related to the sampled businesses
    filtered_df = df[df['business'].isin(sampled_businesses)]
    print(f"Filtered data to {len(filtered_df)} rows related to {num_businesses} unique businesses.")

    # Modify file name
    num_ratings = len(filtered_df)
    num_users = len(filtered_df['user'].drop_duplicates())
    output_file = f"{output_dir}/ratings_{num_businesses}.csv" 

    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered data saved to '{output_file}'.")

    # Save statistics
    with open(f"{output_dir}/stats_{num_businesses}.txt", "w") as file:
        file.write(f"Sample contains {num_businesses} businesses, {num_ratings} ratings, {num_users} users.")

    return output_file

# Filter metadata for businesses present in a rating subset and save it
def save_filtered_subset_of_metadata(csv_file, json_file, output_dir):
    """
    Given a subset of ratings via csv_file, goes through the json_file 
    business metadata and saves the metadata of businesses rated in csv_file 
    to output_file 

    Arguments
        csv_file    : path to ratings for a subset of the metadata
        json_file   : path to full business metadata
        output_file : directory to save filtered business metadata

    Returns: 
        None
    """
    
    try:
        ratings_df = pd.read_csv(csv_file)
        unique_businesses = ratings_df['business'].unique()  # Get unique business names
        print(f"Loaded {len(unique_businesses)} unique businesses from '{csv_file}'.")
    except FileNotFoundError:
        print(f"Error: The file '{csv_file}' was not found.")
        return

    try:
        with open(json_file, 'r') as file:
            business_data = [json.loads(line) for line in file]
            print(f"Loaded {len(business_data)} entries from '{json_file}'.")
    except FileNotFoundError:
        print(f"Error: The file '{json_file}' was not found.")
        return
    except json.JSONDecodeError as e:
        print(f"Error: The file '{json_file}' contains invalid JSON.")
        print(f"{e}")
        return

    # Filter JSON data for businesses that match names in the filtered ratings CSV
    filtered_businesses = []

    # Extract information based on business names
    for business in business_data:
        if business.get('gmap_id') in unique_businesses:
            filtered_businesses.append(business)

    # Check for duplicates based on 'gmap_id'
    gmap_ids = [business.get('gmap_id') for business in filtered_businesses]
    duplicate_gmap_ids = [gmap_id for gmap_id in set(gmap_ids) if gmap_ids.count(gmap_id) > 1]

    unique_businesses_dict = {}
    for business in filtered_businesses:
        gmap_id = business.get('gmap_id')
        if gmap_id not in unique_businesses_dict:
            unique_businesses_dict[gmap_id] = business

    filtered_businesses_unique = list(unique_businesses_dict.values())

    # Modify file name
    num_ratings = len(ratings_df)
    num_businesses = len(unique_businesses) 
    output_file = f"{output_dir}/metadata_{num_businesses}.csv"

    if filtered_businesses_unique:
        df = pd.json_normalize(filtered_businesses_unique)  

        df.to_csv(output_file, index=False, encoding='utf-8')  
        print(f"Filtered business information saved to '{output_file}'.")
    else:
        print("No matching businesses found in the JSON data.")

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
    subset_list = [100, 1000, 10000, 100000]

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
        ratings_subset_file = save_subset_of_ratings(raw_ratings_path, data_dir, num_businesses)
        save_filtered_subset_of_metadata(ratings_subset_file, raw_metadata_path, data_dir)

if __name__ == "__main__":
    fetch_raw_google_reviews_data()
    save_all_subsets()

