from pymongo import MongoClient
import pandas as pd

file_path = '/Users/hoon/Downloads/newNba.csv'  # Replace with your actual file path
data = pd.read_csv(file_path)

# Remove duplicates while keeping the first occurrence
new_nba_data = data.drop_duplicates(subset='NAME', keep='first')


# Connect to MongoDB
client = MongoClient("mongodb+srv://shoon9525:WmOMn1vTg65pnXID@cluster0.iaom9xh.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client["NBA2023"]  # Create or use existing database
collection  = db["newCol"]  # Create or use existing collection
# Set player's name as the index of the DataFrame


new_nba_data.set_index('NAME', inplace=True)

# Convert DataFrame to dictionary format suitable for MongoDB
# Include the index (player's name) as the _id field
data_dict = new_nba_data.reset_index().rename(columns={'NAME': '_id'}).to_dict("records")


# Insert data into MongoDB
collection.insert_many(data_dict)

print("Data uploaded successfully!")
