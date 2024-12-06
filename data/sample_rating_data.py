import pandas as pd

def extract_business_entries(input_file, output_file, num_businesses=25000):
    
    try:
        df = pd.read_csv(input_file)
        print(f"Loaded data from {input_file} with {len(df)} rows.")
    except FileNotFoundError:
        print(f"Error: The file {input_file} was not found.")
        return

    if 'business' not in df.columns:
        print("Error: 'business' column is not present in the CSV file.")
        return

    
    unique_businesses = df['business'].drop_duplicates()
    if len(unique_businesses) < num_businesses:
        print(f"Warning: The dataset contains only {len(unique_businesses)} unique businesses.")
        num_businesses = len(unique_businesses)
    
    sampled_businesses = unique_businesses.sample(n=num_businesses, random_state=42)

    
    filtered_df = df[df['business'].isin(sampled_businesses)]
    print(f"Filtered data to {len(filtered_df)} rows related to {num_businesses} unique businesses.")

    
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered data saved to '{output_file}'.")

input_file = 'rating-Georgia.csv'
output_file = 'filtered_ratings_25k.csv'
extract_business_entries(input_file, output_file)