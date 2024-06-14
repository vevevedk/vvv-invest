import os
import pandas as pd

def extract_info_from_filename(file_path):
    file_name = os.path.basename(file_path).split('.')[0]  # Get the file name without the extension
    
    # Split the filename by '-' and check if it has the expected number of parts
    parts = file_name.split('-')
    if len(parts) < 3:
        print("Filename:", file_name)
        raise ValueError("Filename does not have the expected format")

    # Extracting the ticker
    ticker = parts[0]
    
    # Extracting the expiration date
    start_idx_exp = file_name.find("exp-") + 4  # +4 to move past "exp-"
    expiration_date = file_name[start_idx_exp:start_idx_exp + 10]
    expiration_date = expiration_date.replace("-", "")  # Removing '-'
    
    # Extracting the updated date
    start_idx_date = file_name.rfind("-") - 5  # +1 to move past the last "-"
    updated_date_raw = file_name[start_idx_date:start_idx_date + 10]
    updated_date_parts = updated_date_raw.split("-")
    if len(updated_date_parts) != 3:
        print("Updated date:", updated_date_parts)
        raise ValueError("Updated date does not have the expected format")
    updated_date = updated_date_parts[2] + updated_date_parts[0] + updated_date_parts[1]  # Reformatting to yyyymmdd
    
    return ticker, expiration_date, updated_date

def process_file(file_path):
    print("processing file")
    data = pd.read_csv(os.getcwd()+ "/csv/" + file_path)
    
    ticker, expiration_date, updated_date = extract_info_from_filename(file_path)
    
    # Check if 'Symbol' column exists before filtering
    if 'Symbol' in data.columns:
        data = data[~data['Symbol'].str.contains("Downloaded from Barchart", na=False)]
    
    # Check if 'Strike' column exists before filtering
    if 'Strike' in data.columns:
        data = data[~data['Strike'].str.contains("Downloaded from Barchart", na=True)]
    
    # Prepending the columns
    data.insert(0, 'Updated Date', updated_date)
    data.insert(1, 'Expiration Date', expiration_date)
    data.insert(2, 'Ticker', ticker)
    
    return data

# Get a list of all CSV files in the current working directory
all_files = os.listdir(os.getcwd() + "/csv")
csv_files = [file for file in all_files if file.endswith('.csv')]

# Create an empty master DataFrame
master_df = pd.DataFrame()

# Process each file and append to master_df
for csv_file in csv_files:
    temp_df = process_file(csv_file)
    master_df = pd.concat([master_df, temp_df], ignore_index=True)

# Print head of master df 
master_df.head()

# Save master df to csv
master_df.to_csv("options_chain_data_combined.csv", index=False)
print("data saved to csv file")
