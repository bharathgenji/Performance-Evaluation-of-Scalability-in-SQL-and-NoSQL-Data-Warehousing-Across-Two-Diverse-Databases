import mysql.connector
import os
import transform_into_json as tf
import executeAdventureWorksSQLandMongoDB
import executeGamesRecommendationMYSQLandMongoDB
import insertDataintoMongoDBRecommendation
import InsertDataintoMYSQLRecommendation as IDMR

# Current working directory
cwd = os.getcwd()
db_config = {
    'user': 'HideaJ',
    'password': '',
    'host': 'localhost',
    'database': 'newschema'
}


def startProgram():
    try:
        # Inserting data into Adventureworks using Adventureworks.sql
        # Establishing a database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        # Read and execute SQL file
        with open(os.path.join(cwd, 'AdventureWorks2019.sql'), 'r') as file:
            sql_script = file.read()

        # MySQL connector does not support multi-statement by default, so we split and run them one by one
        commands = sql_script.split(';')

        for command in commands:
            # Ensure that the command is not empty (especially the last split will be empty)
            if command.strip() != '':
                cursor.execute(command)

        # Committing any changes if made
        conn.commit()

        # Closing the connection
        cursor.close()
        conn.close()

        print(f"Executed SQL script from {os.path.join(cwd, 'AdventureWorks2019.sql')}")

    except Exception as e:
        print(f"An error occurred while executing the AdventureWorks2019.sql script: {e}")
        return

    try:
        # Loading data into MongoDB ( AdventureWorks)
        result=tf.main()
        print("Loading data into MongoDB ( AdventureWorks): ",result)
    except Exception as e:
        print(f"An error occurred while loading data into MongoDB for AdventureWorks: {e}")
        return

    try:
        # Insert Data for Recommendation Table MongoDB
        result=insertDataintoMongoDBRecommendation.main()
        print("Insert Data for Recommendation Table MongoDB :",result)
    except Exception as e:
        print(f"An error occurred while inserting data into MongoDB for Recommendations: {e}")
        return

    try:
        # Insert Data for Recommendation Table MYSQL
        IDMR.main()
        print("Insert Data for Recommendation Table MYSQL :")
    except Exception as e:
        print(f"An error occurred while inserting data into MySQL for Recommendations: {e}")
        return

    try:
        # Execute Queries for Recommendation
        result=executeGamesRecommendationMYSQLandMongoDB.main()
        print("Execute Queries for Recommendation: ",result)

    except Exception as e:
        print(f"An error occurred while executing queries for Games Recommendation in MySQL and MongoDB: {e}")
        return

    try:
        # Execute Queries for AdventureWorks
        result=executeAdventureWorksSQLandMongoDB.main()
        print("Execute Queries for AdventureWorks :",result)
    except Exception as e:
        print(f"An error occurred while executing queries for AdventureWorks in SQL and MongoDB: {e}")
        return

    print("Program executed successfully")


# Run the program
startProgram()
