SELECT *
FROM sales_salesorderheader;
SELECT Color, COUNT(*)
FROM production_product
GROUP BY Color;
-- Order Detail Table with Calculated Profit by LineItem
-- Profit is Calculated by subtracting UnitPrice (at time of Sale) by Standard Cost (at time of Sale)
-- Promotional Items are excluded as clearance sales often sell at well below cost
SELECT SalesOrderID
	, a.ProductID
	, c.Name
	, OrderQty
	, UnitPrice
	, b.StandardCost
	, a.ModifiedDate
	, UnitPrice - b.StandardCost as UnitProfit
	, OrderQty*(UnitPrice - b.StandardCost) as LineTotalProfit 
FROM Sales_SalesOrderDetail as a
LEFT JOIN (SELECT ProductID
		, StartDate
	,CASE
		WHEN EndDate IS NULL THEN NOW()
		ELSE EndDate
	END as EndDate
		, StandardCost
		, ModifiedDate
	FROM Production_ProductCostHistory)b
ON a.ProductID = b.ProductID
AND a.ModifiedDate >= StartDate
AND a.ModifiedDate < EndDate
LEFT JOIN Production_Product as c
ON a.ProductID = c.ProductID
WHERE SpecialOfferID = 1
ORDER BY SalesOrderID;
-- Product Sold Most often by each Sales Person
-- Note that these items are apparel, NOT bikes. These items do not have much profit marginne and are often sold at a loss as a loss-leader.
SELECT MaxItems.SalesPersonID
	, MaxItems.MostSold
	, SalesAgg.ProductID
	, c.Name
FROM (
	SELECT SalesPersonID
	, MAX(TotalSold) as MostSold
FROM (
	SELECT b.SalesPersonID
		, ProductID
		, SUM(OrderQty) as TotalSold
	FROM Sales_SalesOrderDetail as a
	LEFT JOIN Sales_SalesOrderHeader as b
	ON a.SalesOrderId = b.SalesOrderID
	WHERE SalesPersonID is NOT NULL
	GROUP BY b.SalesPersonID, ProductID
)ItemTotals
GROUP BY SalesPersonID
)MaxItems
LEFT JOIN (
	SELECT b.SalesPersonID
		, ProductID
		, SUM(OrderQty) as TotalSold
	FROM Sales_SalesOrderDetail as a
	LEFT JOIN Sales_SalesOrderHeader as b
	ON a.SalesOrderId = b.SalesOrderID
	WHERE SalesPersonID is NOT NULL
	GROUP BY b.SalesPersonID, ProductID
)SalesAgg
ON SalesAgg.SalesPersonID = MaxItems.SalesPersonID
AND SalesAgg.TotalSold = MaxItems.MostSold
LEFT JOIN Production_Product as c
ON c.ProductID = SalesAgg.ProductID;
-- Find the top ten items by profit generation in the history of the DB
-- Order Detail Table with Calculated Profit by LineItem
-- Note: The road-250 & road-450 serries bike underperformed significantly versus their -150 counterpart
SELECT ProductID
    , Name
    , SUM(LineTotalProfit) as HistoricalProfit
FROM (
    SELECT a.ProductID as ProductID
        , c.Name
        , OrderQty
        , UnitPrice
        , b.StandardCost
        , a.ModifiedDate
        , UnitPrice - b.StandardCost as UnitProfit
        , OrderQty*(UnitPrice - b.StandardCost) as LineTotalProfit 
    FROM Sales_SalesOrderDetail as a
    LEFT JOIN (SELECT ProductID
            , StartDate
            , CASE
                WHEN EndDate IS NULL THEN NOW()
                ELSE EndDate
              END as EndDate
            , StandardCost
            , ModifiedDate
        FROM Production_ProductCostHistory) b
    ON a.ProductID = b.ProductID
    AND a.ModifiedDate >= StartDate
    AND a.ModifiedDate < EndDate
    LEFT JOIN Production_Product as c
    ON a.ProductID = c.ProductID
    WHERE SpecialOfferID = 1
) ProfitTable
GROUP BY ProductId, Name
ORDER BY HistoricalProfit DESC
LIMIT 10;
-- Bottom Ten
SELECT ProductID
    , Name
    , SUM(LineTotalProfit) as HistoricalProfit
FROM (
    SELECT a.ProductID as ProductID
        , c.Name
        , OrderQty
        , UnitPrice
        , b.StandardCost
        , a.ModifiedDate
        , UnitPrice - b.StandardCost as UnitProfit
        , OrderQty*(UnitPrice - b.StandardCost) as LineTotalProfit 
    FROM Sales_SalesOrderDetail as a
    LEFT JOIN (SELECT ProductID
            , StartDate
            , CASE
                WHEN EndDate IS NULL THEN NOW()
                ELSE EndDate
              END as EndDate
            , StandardCost
            , ModifiedDate
        FROM Production_ProductCostHistory) b
    ON a.ProductID = b.ProductID
    AND a.ModifiedDate >= StartDate
    AND a.ModifiedDate < EndDate
    LEFT JOIN Production_Product as c
    ON a.ProductID = c.ProductID
    WHERE SpecialOfferID = 1
) ProfitTable
GROUP BY ProductId, Name
ORDER BY HistoricalProfit ASC
LIMIT 10;