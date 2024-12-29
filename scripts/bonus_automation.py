import io
import boto3
import pandas as pd
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')

# Define bucket and file details
bucket_name = 'bda-mini-project-bucket'
file_name = 'dataset/games.csv'

#Download data from S3 to a BytesIO buffer
file_buffer = io.BytesIO()
s3.download_fileobj(bucket_name, file_name, file_buffer)
file_buffer.seek(0)  # Reset buffer position to the start

# Load data into pandas DataFrame
df = pd.read_csv(file_buffer)

#-----------------Transformations-------------------
# Drop rows with null values in specific columns
columns_to_drop_nulls = ['Name', 'Full audio languages', 'Header image', 'Metacritic score', 'Negative', 'Recommendations']
df.dropna(subset=columns_to_drop_nulls, inplace=True)

# Drop columns
columns_to_drop = ['About the game', 'Reviews', 'Website', 'Support url', 'Support email', 'Metacritic url', 'Score rank', 'Tags', 'Screenshots', 'Notes']
df.drop(columns=columns_to_drop, inplace=True)

# Impute null values in 'Movies' column
df['Movies'].fillna('No movies', inplace=True)

# Handle null values in 'Developers', 'Publishers', 'Categories', 'Genres'
columns_to_impute = ['Developers', 'Publishers', 'Categories', 'Genres']
for column in columns_to_impute:
    df[column].fillna('Unknown', inplace=True)

# Drop the row with the highest price
df.sort_values(by="Price", ascending=False, inplace=True)
df = df[df['Price'] != df['Price'].iloc[0]]

#------------------New Columns-------------------
# Convert 'Release date' to datetime and calculate 'Game age'

def convert_to_datetime(date_str):
    if pd.isna(date_str):
        return pd.NaT
    
    # Try parsing with the full format
    date_formats = ['%b %d, %Y', '%Y-%m-%d', '%b %Y']
    for fmt in date_formats:
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    
    # If none of the formats match, return NaT
    return pd.NaT

df['Release date'] = df['Release date'].apply(convert_to_datetime)

# Calculate 'Game age' for rows where 'Release date' is not NaT
current_date = pd.Timestamp.now()
df['Game age'] = (current_date - df['Release date']).dt.days / 365.25
df['Game age'] = df['Game age'].fillna(0).round(2)

df['Game age'] = ((datetime.now() - df['Release date']).dt.days / 365).round(2)

# Add 'Price Category' column
df['Price category'] = pd.cut(df['Price'], bins=[-float('inf'), 10, 50, float('inf')], labels=['Low', 'Medium', 'High'])

# Add 'Popularity Tier' column
df['PN ratio'] = df.apply(lambda row: row['Positive'] if row['Negative'] == 0 else row['Positive'] / row['Negative'], axis=1)
df['Popularity tier'] = pd.cut(df['PN ratio'], 
                                bins=[-float('inf'), 1, 2, 5, 10, float('inf')], 
                                labels=['Niche', 'Average', 'Rising', 'Popular', 'Elite'],
                                right=False)

# Save processed DataFrame to CSV
output_buffer = io.BytesIO()
df.to_csv(output_buffer, index=False)
output_buffer.seek(0)  # Reset buffer position to the start

#Upload processed data back to S3
s3.upload_fileobj(output_buffer, bucket_name, 'processed/processed_steam_games.csv')
print("Stored new dataset back to S3 bucket!")