# %%
import csv
import pymysql
import time
from pymongo import MongoClient
import time
import matplotlib.pyplot as plt

def main():
    game_time_list_sql = []

    # MYSQL Database connection
    host = 'localhost'
    user = 'HideaJ'
    password = ''
    db_name = 'game_recommendation'
    conn = pymysql.connect(host=host, user=user, passwd=password, db=db_name)
    cursor = conn.cursor()




    # Queries
    queries = [
        "SELECT * FROM games",
        "SELECT * FROM games WHERE win = 'TRUE'",
        "SELECT rating, COUNT(*) FROM games GROUP BY rating",
        "SELECT g.title, u.user_id FROM games g JOIN recommendations r ON g.app_id = r.app_id JOIN users u ON r.user_id = u.user_id",
        "SELECT * FROM games WHERE app_id IN (SELECT app_id FROM recommendations WHERE hours > (SELECT AVG(hours) FROM recommendations))",
        "SELECT g.title, u.user_id, r.is_recommended FROM games g JOIN recommendations r ON g.app_id = r.app_id JOIN users u ON r.user_id = u.user_id WHERE r.is_recommended = TRUE AND g.linux = 'TRUE'"
    ]


    # query with no limits
    for query in queries:
        # Ensure that the query is not empty
        start_time = time.time()  # Start timer
        cursor.execute(query)
        elapsed_time = time.time() - start_time  # Stop timer
        game_time_list_sql.append(elapsed_time)

        # Clear the result
        if query.lower().startswith('select'):
            cursor.fetchall()

        print(f"Query executed in {elapsed_time:.2f} seconds")

    # Commit and close
    conn.commit()
    cursor.close()
    conn.close()


    print(game_time_list_sql)



    # Initializing Mongo Client and gametimelist as well as DB
    game_time_list_mongo = []
    client = MongoClient('mongodb://localhost:27017/')
    db = client['game']

    #1



    # start time
    start_time = time.time()

    # operate query
    all_games = db.games.find({})

    # end time
    end_time = time.time()

    result1 = end_time - start_time
    game_time_list_mongo.append(result1)

    # operate time
    print(f"Query executed in {end_time - start_time} seconds")



    #2



    # start time
    start_time = time.time()

    # operate query
    games_win_true = db.games.find({ "win": True })

    # end time
    end_time = time.time()

    result2 = end_time - start_time
    game_time_list_mongo.append(result2)

    # operate time
    print(f"Query executed in {end_time - start_time} seconds")




    # start time
    start_time = time.time()

    # operate query
    results = db.games.aggregate([
        { "$group": { "_id": "$rating", "count": { "$sum": 1 } } }
    ])

    # end time
    end_time = time.time()

    result3 = end_time - start_time
    game_time_list_mongo.append(result3)

    # operate time
    print(f"Query executed in {end_time - start_time} seconds")




    # start time
    start_time = time.time()

    # operate query
    results = db.games.aggregate([
        {
            "$lookup": {
                "from": "recommendations",
                "localField": "app_id",
                "foreignField": "app_id",
                "as": "recommendations"
            }
        },
        { "$unwind": "$recommendations" },
        {
            "$lookup": {
                "from": "users",
                "localField": "recommendations.user_id",
                "foreignField": "user_id",
                "as": "user"
            }
        },
        { "$unwind": "$user" },
        {
            "$project": {
                "title": 1,
                "user_id": "$user.user_id"
            }
        }
    ])

    # end time
    end_time = time.time()

    result4 = end_time - start_time
    game_time_list_mongo.append(result4)

    # operate time
    print(f"Query executed in {end_time - start_time} seconds")


    # define pipeline
    pipeline = [
        {
            "$group": {
                "_id": None,
                "avgHours": { "$avg": "$hours" }
            }
        },
        {
            "$lookup": {
                "from": "recommendations",
                "let": { "avgHours": "$avgHours" },
                "pipeline": [
                    { "$match": { "$expr": { "$gt": ["$hours", "$$avgHours"] } } },
                    { "$project": { "app_id": 1 } }
                ],
                "as": "filteredRecommendations"
            }
        },
        { "$unwind": "$filteredRecommendations" },
        { "$replaceRoot": { "newRoot": "$filteredRecommendations" } },
        {
            "$lookup": {
                "from": "games",
                "localField": "app_id",
                "foreignField": "app_id",
                "as": "gameDetails"
            }
        },
        { "$unwind": "$gameDetails" },
        { "$replaceRoot": { "newRoot": "$gameDetails" } }
    ]

    # Start time
    start_time = time.time()

    # operate pipeline
    results = db.recommendations.aggregate(pipeline)

    # end time
    end_time = time.time()

    result5 = end_time - start_time
    game_time_list_mongo.append(result5)

    # operate time
    print(f"Query executed in {end_time - start_time} seconds")



    # atart time
    start_time = time.time()

    # operate query
    results = db.games.aggregate([
        { "$match": { "linux": 'TRUE' } },
        {
            "$lookup": {
                "from": "recommendations",
                "localField": "app_id",
                "foreignField": "app_id",
                "as": "recommendations"
            }
        },
        { "$unwind": "$recommendations" },
        { "$match": { "recommendations.is_recommended": True } },
        {
            "$lookup": {
                "from": "users",
                "localField": "recommendations.user_id",
                "foreignField": "user_id",
                "as": "user"
            }
        },
        { "$unwind": "$user" },
        {
            "$project": {
                "title": 1,
                "user_id": "$user.user_id",
                "is_recommended": "$recommendations.is_recommended"
            }
        }
    ])

    # end time
    end_time = time.time()

    result6 = end_time - start_time
    game_time_list_mongo.append(result6)

    # operate time
    print(f"Query executed in {end_time - start_time} seconds")


    print(game_time_list_mongo)

    # Number of queries
    num_queries = range(1, len(game_time_list_sql) + 1)

    # Plotting
    plt.figure(figsize=(10, 5))
    plt.plot(num_queries, game_time_list_sql, marker='o', color='b', label='SQL')
    plt.plot(num_queries, game_time_list_mongo, marker='x', color='r', label='MongoDB')
    plt.title('SQL vs MongoDB Query Execution Time')
    plt.xlabel('Query Number')
    plt.ylabel('Execution Time (seconds)')
    plt.xticks(num_queries)
    plt.legend()
    plt.grid(True)
    plt.show()

    return 1