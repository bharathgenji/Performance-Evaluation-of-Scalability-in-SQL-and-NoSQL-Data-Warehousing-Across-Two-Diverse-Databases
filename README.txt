Project Title: Performance Evaluation of Scalability in SQL and NoSQL Data Warehousing Across Two Diverse Databases

Description
This project focuses on processing and analyzing data from two main sources: a game recommendation dataset and the AdventureWorks2019 dataset. The project involves:

1.Loading data into MySQL and MongoDB databases.
2.Executing various SQL and MongoDB queries for data manipulation and analysis.
3.Comparing the performance of data operations between SQL and MongoDB.

Prerequisites
Python 3.x
MySQL Server
MongoDB
pip (Python package manager)


Installation
Step 1: Setting Up the Environment
It is recommended to use a virtual environment for Python projects to manage dependencies.
Create a Virtual Environment

python -m venv venv

Step 2: Install Required Packages
Install all required Python packages listed in requirements.txt.

pip install -r requirements.txt

Running the Project
Step 1: Database Configuration
Ensure MySQL and MongoDB are running on your system. Configure the connection parameters in the Python scripts to connect to your MySQL and MongoDB instances.

Step 2: Execute SQL Script for AdventureWorks
Run the provided Python script to execute the SQL commands in AdventureWorks2019.sql.

executeAdventureWorksSQLandMongoDB()

Step 3: Load Data into MongoDB
Use the script to load data into MongoDB collections from the respective CSV files.

insertDataintoMongoDBRecommendation()

Step 4: Load Data into MySQL
Run the script to insert data into MySQL tables from the CSV files.

IDMR.main()

Step 5: Execute Queries and Analysis
Run the scripts to execute queries and analyze the data in both SQL and MongoDB.
executeGamesRecommendationMYSQLandMongoDB()
executeAdventureWorksSQLandMongoDB()

Step 6: Deactivate the Virtual Environment
Once you are done, you can deactivate the virtual environment.
deactivate
