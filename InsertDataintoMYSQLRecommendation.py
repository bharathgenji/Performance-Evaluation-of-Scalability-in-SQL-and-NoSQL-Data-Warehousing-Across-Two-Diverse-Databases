import csv
import pymysql
import time
import os

# MySQL Database connection
host = 'localhost'
user = 'HideaJ'
password = ''
db_name = 'game_recommendation'
conn = pymysql.connect(host=host, user=user, passwd=password, db=db_name)
cursor = conn.cursor()

cwd = os.getcwd()
# Create tables if they don't exist
def create_tables():
    create_games_table = """
    CREATE TABLE IF NOT EXISTS games (
        app_id INT PRIMARY KEY,
        title VARCHAR(255),
        date_release DATE,
        win VARCHAR(255),
        mac VARCHAR(255),
        linux VARCHAR(255),
        rating VARCHAR(255),
        positive_ratio INT,
        user_reviews INT,
        price_final FLOAT,
        price_original FLOAT,
        discount FLOAT,
        steam_deck VARCHAR(255)
    );
    """
    create_recommendations_table = """
    CREATE TABLE IF NOT EXISTS recommendations (
        app_id INT,
        helpful INT,
        funny INT,
        date DATE,
        is_recommended VARCHAR(255),
        hours FLOAT,
        user_id INT,
        review_id INT PRIMARY KEY,
        FOREIGN KEY (app_id) REFERENCES games(app_id)
    );
    """
    create_users_table = """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        products INT,
        reviews INT
    );
    """

    cursor.execute(create_games_table)
    cursor.execute(create_recommendations_table)
    cursor.execute(create_users_table)



# Function to insert a row into the games table and measure time
def insert_into_games(row):
    start_time = time.time()
    sql = """INSERT INTO games (app_id, title, date_release, win, mac, linux, rating, positive_ratio, 
                                user_reviews, price_final, price_original, discount, steam_deck) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql, row)
    return time.time() - start_time

# Function to insert a row into the users table and measure time
def insert_into_users(row):
    start_time = time.time()
    sql = """INSERT INTO users (user_id, products, reviews) 
             VALUES (%s, %s, %s)"""
    cursor.execute(sql, row)
    return time.time() - start_time

# Function to insert a row into the recommendations table and measure time
def insert_into_recommendations(row):
    start_time = time.time()
    sql = """INSERT INTO recommendations (app_id, helpful, funny, date, is_recommended, hours, user_id, review_id) 
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""
    cursor.execute(sql, row)
    return time.time() - start_time



def main():
    # Variables to store total time for each operation
    total_time_games = 0
    total_time_users = 0
    total_time_recommendations = 0
    # Call the function to create tables
    create_tables()
    # Reading and inserting data into games table
    with open(os.path.join(cwd, 'Games_RecommendationDataset/games.csv'), 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header
        for row in csvreader:
            total_time_games += insert_into_games(row)

    # Reading and inserting data into users table
    with open(os.path.join(cwd, 'Games_RecommendationDataset/users.csv'), 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header
        for row in csvreader:
            total_time_users += insert_into_users(row)

    # Reading and inserting data into recommendations table
    with open(os.path.join(cwd, 'Games_RecommendationDataset/recommendations.csv'), 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the header
        for row in csvreader:
            total_time_recommendations += insert_into_recommendations(row)

    # Print total times
    print(f"Total time to insert into games: {total_time_games} seconds")
    print(f"Total time to insert into users: {total_time_users} seconds")
    print(f"Total time to insert into recommendations: {total_time_recommendations} seconds")
    # Close the database connection
    cursor.close()
    conn.close()


