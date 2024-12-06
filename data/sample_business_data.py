import pandas as pd
import json

def extract_business_info(csv_file, json_file, output_file):
    
    try:
        ratings_df = pd.read_csv(csv_file)
        unique_businesses = ratings_df['business'].unique()
        print(f"Loaded {len(unique_businesses)} unique businesses from '{csv_file}'.")
    except FileNotFoundError:
        print(f"Error: The file '{csv_file}' was not found.")
        return

    
    try:
        with open(json_file, 'r', encoding='utf-8') as file:
            business_data = json.load(file)
            print(f"Loaded {len(business_data)} entries from '{json_file}'.")
    except FileNotFoundError:
        print(f"Error: The file '{json_file}' was not found.")
        return
    except json.JSONDecodeError:
        print(f"Error: The file '{json_file}' contains invalid JSON.")
        return


    filtered_businesses = []


    for business in business_data:
        if business.get('gmap_id') in unique_businesses:
            filtered_businesses.append(business)

    
    gmap_ids = [business.get('gmap_id') for business in filtered_businesses]
    duplicate_gmap_ids = [gmap_id for gmap_id in set(gmap_ids) if gmap_ids.count(gmap_id) > 1]

    unique_businesses_dict = {}
    for business in filtered_businesses:
        gmap_id = business.get('gmap_id')
        if gmap_id not in unique_businesses_dict:
            unique_businesses_dict[gmap_id] = business


    filtered_businesses_unique = list(unique_businesses_dict.values())

    if filtered_businesses_unique:
        df = pd.json_normalize(filtered_businesses_unique)  

        df.to_csv(output_file, index=False, encoding='utf-8')  
        print(f"Filtered business information saved to '{output_file}'.")
    else:
        print("No matching businesses found in the JSON data.")


csv_file = 'filtered_ratings_25k.csv'   
json_file = 'fixed-meta-Georgia.json'    
output_file = 'matched_businesses_25k.csv'  

extract_business_info(csv_file, json_file, output_file)
