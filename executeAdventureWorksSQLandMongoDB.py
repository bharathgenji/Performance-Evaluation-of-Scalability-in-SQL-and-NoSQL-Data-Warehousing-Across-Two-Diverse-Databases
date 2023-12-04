
import mysql.connector
import time
from pymongo import MongoClient
import matplotlib.pyplot as plt

def main():
    adv_time_list_sql = []
    adv_time_list_mongo = []
    # Database connection parameters
    config = {
        'user': 'HideaJ',
        'password': '',
        'host': 'localhost',
        'database': 'newschema',
        'raise_on_warnings': True
    }
    
    # Establishing a database connection
    cnx = mysql.connector.connect(**config)
    cursor = cnx.cursor(buffered=True)
    
    # Read SQL file
    with open('advwork_sql.sql', 'r') as file:
        sql_queries = file.read()
    
    # Split multiple queries
    queries = sql_queries.split(';')
    
    for query in queries:
        # Ensure that the query is not empty
        if query.strip() != '':
            start_time = time.time()  # Start timer
            cursor.execute(query)
            elapsed_time = time.time() - start_time  # Stop timer
            adv_time_list_sql.append(elapsed_time)
    
            # Clear the result
            if query.lower().startswith('select'):
                cursor.fetchall()
    
            print(f"Query executed in {elapsed_time:.2f} seconds")
    
    # Closing the connection
    cursor.close()
    cnx.close()
    
    print(adv_time_list_sql)
    
    
    
    #1
    # Connecting to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['adventure2019']
    
    # query start time
    start_time = time.time()
    
    # query operates
    results = db.sales_salesorderheader.find({})
    
    # query end time
    end_time = time.time()
    
    result1 = end_time - start_time
    adv_time_list_mongo.append(result1)
    
    # calculate the operation time and print
    print(f"Query executed in {end_time - start_time} seconds")
    
    
    
    
    #3
    # Connecting to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['adventure2019'] 
    
    # query start time
    start_time = time.time()
    
    # query operates
    results = db.production_product.aggregate([
        { "$group": { "_id": "$Color", "count": { "$sum": 1 } } }
    ])
    
    # query end time
    end_time = time.time()
    
    result2 = end_time - start_time
    adv_time_list_mongo.append(result2)
    
    # calculate the operation time and print
    print(f"Query executed in {end_time - start_time} seconds")
    
    
    
    
    
    # Connecting to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['adventure2019'] 
    
    # define pipeline
    pipeline = [
        { "$match": { "SpecialOfferID": 1 } },
        { 
            "$lookup": {
                "from": "Production_ProductCostHistory",
                "let": { "product_id": "$ProductID", "modified_date": "$ModifiedDate" },
                "pipeline": [
                    { "$match": { "$expr": { 
                        "$and": [
                            { "$eq": ["$ProductID", "$$product_id"] },
                            { "$lte": ["$StartDate", "$$modified_date"] },
                            { "$or": [
                                { "$gte": ["$EndDate", "$$modified_date"] },
                                { "$eq": ["$EndDate", None] }
                            ]}
                        ]
                    }}},
                    { "$project": { "StandardCost": 1, "EndDate": 1 } }
                ],
                "as": "CostHistory"
            }
        },
        { "$unwind": "$CostHistory" },
        { 
            "$lookup": {
                "from": "Production_Product",
                "localField": "ProductID",
                "foreignField": "ProductID",
                "as": "ProductDetails"
            }
        },
        { "$unwind": "$ProductDetails" },
        { 
            "$project": {
                "SalesOrderID": 1,
                "ProductID": 1,
                "Name": "$ProductDetails.Name",
                "OrderQty": 1,
                "UnitPrice": 1,
                "StandardCost": "$CostHistory.StandardCost",
                "ModifiedDate": 1,
                "UnitProfit": { "$subtract": ["$UnitPrice", "$CostHistory.StandardCost"] },
                "LineTotalProfit": { "$multiply": ["$OrderQty", { "$subtract": ["$UnitPrice", "$CostHistory.StandardCost"] }] }
            }
        },
        { "$sort": { "SalesOrderID": 1 } }
    ]
    
    # query start time
    start_time = time.time()
    
    # query operates
    results = db.Sales_SalesOrderDetail.aggregate(pipeline)
    
    # query end time
    end_time = time.time()
    
    result3 = end_time - start_time
    adv_time_list_mongo.append(result3)
    
    # calculate the operation time and print
    print(f"Query executed in {end_time - start_time} seconds")
    
    
    
    
    # Connecting to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['adventure2019']
    
    # define pipeline
    pipeline = [
        { 
            "$group": {
                "_id": {
                    "SalesPersonID": "$SalesPersonID",
                    "ProductID": "$ProductID"
                },
                "TotalSold": { "$sum": "$OrderQty" }
            }
        },
        { 
            "$project": {
                "SalesPersonID": "$_id.SalesPersonID",
                "ProductID": "$_id.ProductID",
                "TotalSold": 1
            }
        },
        { "$out": "SalesAggTemp" }
    ]
    # start time
    start_time1 = time.time()
    
    # operate first pipeline
    db.Sales_SalesOrderDetail.aggregate(pipeline)
    
    # end time
    end_time1 = time.time()
    
    # define second pipeline
    pipeline = [
        {
            "$group": {
                "_id": "$SalesPersonID",
                "MostSold": { "$max": "$TotalSold" }
            }
        },
        {
            "$lookup": {
                "from": "SalesAggTemp",
                "localField": "_id",
                "foreignField": "SalesPersonID",
                "as": "SalesAgg"
            }
        },
        { "$unwind": "$SalesAgg" },
        {
            "$match": {
                "$expr": { "$eq": ["$MostSold", "$SalesAgg.TotalSold"] }
            }
        },
        {
            "$lookup": {
                "from": "Production_Product",
                "localField": "SalesAgg.ProductID",
                "foreignField": "ProductID",
                "as": "ProductDetails"
            }
        },
        { "$unwind": "$ProductDetails" },
        {
            "$project": {
                "SalesPersonID": "$_id",
                "MostSold": 1,
                "ProductID": "$SalesAgg.ProductID",
                "Name": "$ProductDetails.Name"
            }
        }
    ]
    
    # start time
    start_time2 = time.time()
    
    # operate pipeline
    results = db.SalesAggTemp.aggregate(pipeline)
    
    # end time
    end_time2 = time.time()
    
    result4 = end_time1+end_time2-start_time1-start_time2
    adv_time_list_mongo.append(result4)
    
    # operate time
    print(f"Query executed in {end_time1+end_time2-start_time1-start_time2} seconds")
    
    
    
    
    # Connecting to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['your_database_name']
    
    # define pipeline
    pipeline = [
        { "$match": { "SpecialOfferID": 1 } },
        {
            "$lookup": {
                "from": "Production_ProductCostHistory",
                "let": { "product_id": "$ProductID", "modified_date": "$ModifiedDate" },
                "pipeline": [
                    { "$match": { "$expr": { 
                        "$and": [
                            { "$eq": ["$ProductID", "$$product_id"] },
                            { "$lte": ["$StartDate", "$$modified_date"] },
                            { "$or": [
                                { "$gte": ["$EndDate", "$$modified_date"] },
                                { "$eq": ["$EndDate", None] }
                            ]}
                        ]
                    }}},
                    { "$project": { "StandardCost": 1 } }
                ],
                "as": "CostHistory"
            }
        },
        { "$unwind": "$CostHistory" },
        {
            "$lookup": {
                "from": "Production_Product",
                "localField": "ProductID",
                "foreignField": "ProductID",
                "as": "ProductDetails"
            }
        },
        { "$unwind": "$ProductDetails" },
        {
            "$project": {
                "ProductID": 1,
                "Name": "$ProductDetails.Name",
                "LineTotalProfit": {
                    "$multiply": [
                        "$OrderQty",
                        { "$subtract": ["$UnitPrice", "$CostHistory.StandardCost"] }
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": { "ProductID": "$ProductID", "Name": "$Name" },
                "HistoricalProfit": { "$sum": "$LineTotalProfit" }
            }
        },
        { "$sort": { "HistoricalProfit": -1 } },
        { "$limit": 10 }
    ]
    
    # Start time
    start_time = time.time()
    
    # operate pipeline
    results = db.Sales_SalesOrderDetail.aggregate(pipeline)
    
    # end time
    end_time = time.time()
    
    result5 = end_time - start_time
    adv_time_list_mongo.append(result5)
    
    # operate time
    print(f"Query executed in {end_time - start_time} seconds")
    
    
    
    
    
    client = MongoClient('mongodb://localhost:27017/')
    db = client['your_database_name']
    
    pipeline = [
        { "$match": { "SpecialOfferID": 1 } },
        {
            "$lookup": {
                "from": "Production_ProductCostHistory",
                "let": { "product_id": "$ProductID", "modified_date": "$ModifiedDate" },
                "pipeline": [
                    { "$match": { "$expr": { 
                        "$and": [
                            { "$eq": ["$ProductID", "$$product_id"] },
                            { "$lte": ["$StartDate", "$$modified_date"] },
                            { "$or": [
                                { "$gte": ["$EndDate", "$$modified_date"] },
                                { "$eq": ["$EndDate", None] }
                            ]}
                        ]
                    }}},
                    { "$project": { "StandardCost": 1 } }
                ],
                "as": "CostHistory"
            }
        },
        { "$unwind": "$CostHistory" },
        {
            "$lookup": {
                "from": "Production_Product",
                "localField": "ProductID",
                "foreignField": "ProductID",
                "as": "ProductDetails"
            }
        },
        { "$unwind": "$ProductDetails" },
        {
            "$project": {
                "ProductID": 1,
                "Name": "$ProductDetails.Name",
                "LineTotalProfit": {
                    "$multiply": [
                        "$OrderQty",
                        { "$subtract": ["$UnitPrice", "$CostHistory.StandardCost"] }
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": { "ProductID": "$ProductID", "Name": "$Name" },
                "HistoricalProfit": { "$sum": "$LineTotalProfit" }
            }
        },
        { "$sort": { "HistoricalProfit": 1 } },
        { "$limit": 10 }
    ]
    
    
    start_time = time.time()
    
    
    results = db.Sales_SalesOrderDetail.aggregate(pipeline)
    
    end_time = time.time()
    
    result6 = end_time - start_time
    adv_time_list_mongo.append(result6)
    
    print(f"Query executed in {end_time - start_time} seconds")
    
    
    print(adv_time_list_mongo)
    
    
    
    # Number of queries executed
    num_queries = range(1, len(adv_time_list_sql) + 1)
    
    # Plotting the graph
    plt.figure(figsize=(10, 5))
    plt.plot(num_queries, adv_time_list_sql, marker='o', color='b', label='SQL')
    plt.plot(num_queries, adv_time_list_mongo, marker='x', color='r', label='MongoDB')
    plt.title('Execution Time Comparison Between SQL and MongoDB Queries')
    plt.xlabel('Query Number')
    plt.ylabel('Time in Seconds')
    plt.xticks(num_queries)
    plt.legend()
    plt.grid(True)
    plt.show()

    return 1