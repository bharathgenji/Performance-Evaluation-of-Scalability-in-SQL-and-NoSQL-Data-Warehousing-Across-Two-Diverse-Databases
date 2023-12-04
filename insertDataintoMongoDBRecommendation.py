from pymongo import MongoClient
import csv
import os
import time

def main():
    # MongoDB connection
    client = MongoClient('mongodb://localhost:27017/')
    db = client['game']

    # Current working directory
    cwd = os.getcwd()

    # Function to insert data into MongoDB collection
    def insert_data_into_mongodb(collection_name, file_path):
        start_time = time.time()
        collection = db[collection_name]
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            csvreader = csv.DictReader(csvfile)
            documents = [row for row in csvreader]
            if documents:
                collection.insert_many(documents)
        return time.time() - start_time

    # File paths
    games_file_path = os.path.join(cwd, 'Games_RecommendationDataset/games.csv')
    users_file_path = os.path.join(cwd, 'Games_RecommendationDataset/users.csv')
    recommendations_file_path = os.path.join(cwd, 'Games_RecommendationDataset/recommendations.csv')

    # Insert data and measure time
    game_time_list_mongo = []

    # Insert into 'games' collection
    time_taken = insert_data_into_mongodb('games', games_file_path)
    game_time_list_mongo.append(('games', time_taken))

    # Insert into 'users' collection
    time_taken = insert_data_into_mongodb('users', users_file_path)
    game_time_list_mongo.append(('users', time_taken))

    # Insert into 'recommendations' collection
    time_taken = insert_data_into_mongodb('recommendations', recommendations_file_path)
    game_time_list_mongo.append(('recommendations', time_taken))

    # Print time taken for each insert operation
    for collection, time_taken in game_time_list_mongo:
        print(f"Time taken to insert data into {collection}: {time_taken} seconds")

    return True