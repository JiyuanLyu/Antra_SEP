-- Jiyuan Lyu's Assingment


-- Problem 1
USE WideWorldImporters
GO

SELECT FullName, People.PhoneNumber, People.FaxNumber, Customers.PhoneNumber AS 'CompanyPhoneNumber', Customers.FaxNumber AS 'CompanyFaxNumber'
FROM Application.People
LEFT JOIN Sales.Customers
ON PersonID = PrimaryContactPersonID OR PersonID = AlternateContactPersonID;


-- Problem 2
USE WideWorldImporters
GO

SELECT CustomerName
FROM Application.People, Sales.Customers
WHERE People.PersonID = Customers.PrimaryContactPersonID
AND People.PhoneNumber = Customers.PhoneNumber;


-- Problem 3
USE WideWorldImporters
GO

SELECT CustomerName
FROM Sales.Customers
WHERE CustomerID IN (
    SELECT CustomerID
    FROM Sales.Orders
    WHERE OrderDate < '2016-01-01');


-- Problem 4
USE WideWorldImporters
GO

SELECT StockItems.StockItemName, SUM(PurchaseOrderLines.OrderedOuters) AS 'TotalQuantityPurchased2013'
FROM Purchasing.PurchaseOrders

LEFT JOIN Purchasing.PurchaseOrderLines
ON PurchaseOrders.PurchaseOrderID = PurchaseOrderLines.PurchaseOrderID

LEFT JOIN Warehouse.StockItems
ON PurchaseOrderLines.StockItemID = StockItems.StockItemID

WHERE PurchaseOrders.OrderDate BETWEEN '2013-01-01' AND '2013-12-31'

GROUP BY StockItems.StockItemName;


-- Problem 5
USE WideWorldImporters
GO

SELECT StockItems.StockItemID, StockItems.StockItemName
FROM Purchasing.PurchaseOrderLines, Warehouse.StockItems
WHERE [Description] LIKE '%__________%'
AND PurchaseOrderLines.StockItemID = StockItems.StockItemID;


-- Problem 6
USE WideWorldImporters
GO

SELECT DISTINCT StockItems.StockItemName
FROM Sales.Customers

LEFT JOIN Application.Cities
ON Customers.DeliveryCityID = Cities.CityID

LEFT JOIN Application.StateProvinces
ON Cities.StateProvinceID = StateProvinces.StateProvinceID

LEFT JOIN Sales.Orders
ON Customers.CustomerID = Orders.CustomerID

LEFT JOIN Sales.OrderLines
ON Orders.OrderID = OrderLines.OrderID

LEFT JOIN Warehouse.StockItems
ON OrderLines.StockItemID = StockItems.StockItemID

WHERE StateProvinces.StateProvinceName NOT IN ('Alabama', 'Georgia')
AND Orders.OrderDate BETWEEN '2014-01-01' AND '2014-12-31';


-- Problem 7
USE WideWorldImporters
GO

SELECT StateProvinces.StateProvinceName,
AVG(DATEDIFF(DAY, Orders.OrderDate, CAST(Invoices.ConfirmedDeliveryTime AS DATE))) AS 'AverageProcessingDays'
FROM Sales.Orders

LEFT JOIN Sales.Invoices
ON Orders.OrderID = Invoices.OrderID

LEFT JOIN Sales.Customers
ON Orders.CustomerID = Customers.CustomerID

LEFT JOIN Application.Cities
ON Customers.DeliveryCityID = Cities.CityID

LEFT JOIN Application.StateProvinces
ON Cities.StateProvinceID = StateProvinces.StateProvinceID

GROUP BY StateProvinces.StateProvinceName;


-- Problem 8
USE WideWorldImporters
GO

SELECT *
FROM
(
    SELECT StateProvinces.StateProvinceName, DATENAME(MONTH, DATEADD( MONTH, MONTH(Orders.OrderDate), -1)) AS 'Month',
    CAST(DATEDIFF(DAY, OrderDate, CAST(Invoices.ConfirmedDeliveryTime AS DATE)) AS int) AS 'ProcessingDays'
    FROM Sales.Orders
    
    LEFT JOIN Sales.Invoices
    ON Orders.OrderID = Invoices.OrderID
    
    LEFT JOIN Sales.Customers
    ON Orders.CustomerID = Customers.CustomerID
    
    LEFT JOIN Application.Cities
    ON Customers.DeliveryCityID = Cities.CityID
    
    LEFT JOIN Application.StateProvinces
    ON Cities.StateProvinceID = StateProvinces.StateProvinceID
    
) AS t1
PIVOT
(
    AVG(processingDays)
    FOR Month IN ([January], [February], [March], [April], [May], [June], [July],
    [August], [September], [October], [November], [December])
) AS pvt1;


-- Problem 9
USE WideWorldImporters
GO

SELECT StockItems.StockItemName
FROM Warehouse.StockItems

LEFT JOIN Purchasing.PurchaseOrderLines
ON StockItems.StockItemID = PurchaseOrderLines.StockItemID

LEFT JOIN Purchasing.PurchaseOrders
ON PurchaseOrderLines.PurchaseOrderID = PurchaseOrders.PurchaseOrderID

LEFT JOIN Sales.OrderLines
ON StockItems.StockItemID = OrderLines.StockItemID

LEFT JOIN Sales.Orders
ON OrderLines.OrderID = Orders.OrderID

WHERE PurchaseOrders.OrderDate BETWEEN '2015-01-01' AND '2015-12-31'
AND Orders.OrderDate BETWEEN '2015-01-01' AND '2015-12-31'

GROUP BY StockItems.StockItemName
HAVING SUM(CAST(PurchaseOrderLines.OrderedOuters AS BIGINT) * CAST(StockItems.QuantityPerOuter AS BIGINT)) > SUM(CAST(OrderLines.Quantity AS BIGINT));


-- Problem 10
USE WideWorldImporters
GO

SELECT CustomerName, Customers.PhoneNumber, FullName AS 'Primary Contact Person'
FROM Sales.Orders
LEFT JOIN Sales.OrderLines
ON Orders.OrderID = OrderLines.OrderID

LEFT JOIN Warehouse.StockItems
ON OrderLines.StockItemID = StockItems.StockItemID

LEFT JOIN Sales.Customers
ON Orders.CustomerID = Customers.CustomerID

LEFT JOIN Application.People
ON Customers.PrimaryContactPersonID = People.PersonID

WHERE StockItemName LIKE '%mug%'
AND Quantity <= 10
AND OrderDate BETWEEN '2016-01-01' AND '2016-12-31';

-- Problem 11
USE WideWorldImporters
GO

SELECT CityName
FROM Application.Cities
WHERE ValidFrom > '2015-01-01';

-- Problem 12
USE WideWorldImporters
GO

SELECT Orders.OrderDate, StockItems.StockItemName, Customers.DeliveryAddressLine1, Customers.DeliveryAddressLine2,
StateProvinces.StateProvinceName, Countries.CountryName, Customers.CustomerName,
People.FullName AS 'PrimaryContactPersonName', Customers.PhoneNumber, OrderLines.Quantity

FROM Sales.Orders

LEFT JOIN Sales.OrderLines
ON Orders.OrderID = OrderLines.OrderID

LEFT JOIN Warehouse.StockItems
ON OrderLines.StockItemID = StockItems.StockItemID

LEFT JOIN Sales.Customers
ON Orders.CustomerID = Customers.CustomerID

LEFT JOIN Application.People
ON Customers.PrimaryContactPersonID = People.PersonID

LEFT JOIN Application.Cities
ON Customers.DeliveryCityID = Cities.CityID

LEFT JOIN Application.StateProvinces
ON Cities.StateProvinceID = StateProvinces.StateProvinceID

LEFT JOIN Application.Countries
ON StateProvinces.CountryID = Countries.CountryID

WHERE Orders.OrderDate = '2014-07-01';


-- Problem 13
USE WideWorldImporters
GO

SELECT StockGroups.StockGroupName,
SUM(CAST(PurchaseOrderLines.OrderedOuters AS BIGINT) * CAST(StockItems.QuantityPerOuter AS BIGINT)) AS 'TotalQuantityPurchased',
SUM(CAST(OrderLines.Quantity AS BIGINT)) AS 'TotalQuantitySold',
SUM(CAST(PurchaseOrderLines.OrderedOuters AS BIGINT)) - SUM(CAST(OrderLines.Quantity AS BIGINT)) AS 'RemainingStockQuantity'
FROM Warehouse.StockItems

LEFT JOIN Warehouse.StockItemStockGroups
ON StockItems.StockItemID = StockItemStockGroups.StockItemID

LEFT JOIN Warehouse.StockGroups
ON StockItemStockGroups.StockGroupID = StockGroups.StockGroupID

LEFT JOIN Purchasing.PurchaseOrderLines
ON StockItems.StockItemID = PurchaseOrderLines.StockItemID

LEFT JOIN Sales.OrderLines
ON StockItems.StockItemID = OrderLines.StockItemID

GROUP BY StockGroups.StockGroupName;

-- Problem 14
USE WideWorldImporters
GO

WITH t1
AS 
(
    SELECT CityID, CityName
    FROM Application.Cities
    WHERE StateProvinceID IN (
        SELECT StateProvinceID
        FROM Application.StateProvinces
        WHERE CountryID IN (
            SELECT CountryID
            FROM Application.Countries
            WHERE CountryName = 'United States'
        )
    )
)

, t2
AS
(
    SELECT Customers.DeliveryCityID, StockItems.StockItemName, 
    COUNT(Orders.OrderID) OVER (PARTITION BY StockItems.StockItemID) AS 'DeliveryTimes'
    FROM Sales.Orders
    LEFT JOIN Sales.OrderLines
    ON Orders.OrderID = OrderLines.OrderID
    LEFT JOIN Warehouse.StockItems
    ON OrderLines.StockItemID = StockItems.StockItemID
    LEFT JOIN Sales.Customers
    ON Orders.CustomerID = Customers.CustomerID
    WHERE Orders.OrderDate BETWEEN '2016-01-01' AND '2016-12-31'

)

, t3
AS
(
    SELECT *,
    RANK() OVER (PARTITION BY DeliveryCityID ORDER BY DeliveryTimes DESC) AS RNK
    FROM t2
)

, t4
AS
(
    SELECT DISTINCT DeliveryCityID, StockItemName, DeliveryTimes --DeliveryCityID, StockItemName
    FROM t3
    WHERE RNK = 1
)

SELECT CityID, CityName, ISNULL(StockItemName, 'No Sales') AS 'MostDeliveryItem2016'
FROM t1
LEFT JOIN t4
ON t1.CityID = t4.DeliveryCityID;


-- Problem 15
USE WideWorldImporters
GO

SELECT OrderID
FROM Sales.Invoices
WHERE JSON_VALUE(ReturnedDeliveryData, '$.Events[2].Event') IS NOT NULL;


-- Problem 16
USE WideWorldImporters
GO

SELECT StockItemName, JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS 'CountryOfManufacture'
FROM Warehouse.StockItems
WHERE JSON_VALUE(CustomFields, '$.CountryOfManufacture') = 'China';

-- Problem 17
USE WideWorldImporters
GO

SELECT SUM(OrderLines.Quantity) AS 'TotalItemSold2015',
JSON_VALUE(CustomFields, '$.CountryOfManufacture') AS 'CountryOfManufacture'
FROM Sales.Orders

INNER JOIN Sales.OrderLines
ON Orders.OrderID = OrderLines.OrderID

INNER JOIN Warehouse.StockItems
ON OrderLines.StockItemID = StockItems.StockItemID

WHERE Orders.OrderDate BETWEEN '2015-01-01' AND '2015-12-31'
GROUP BY JSON_VALUE(StockItems.CustomFields, '$.CountryOfManufacture');


-- Problem 18
USE WideWorldImporters
GO

CREATE VIEW vwGroupQuantitySoldPerYear AS (
    SELECT StockGroupName, [2013], [2014], [2015], [2016], [2017]
    FROM (
        SELECT StockGroups.StockGroupName,
        OrderLines.Quantity,
        Year(Orders.OrderDate) AS 'Year'
        FROM Sales.Orders

        INNER JOIN Sales.OrderLines
        ON Orders.OrderID = OrderLines.OrderID

        INNER JOIN Warehouse.StockItems
        ON OrderLines.StockItemID = StockItems.StockItemID

        INNER JOIN Warehouse.StockItemStockGroups
        ON StockItems.StockItemID = StockItemStockGroups.StockItemID

        INNER JOIN Warehouse.StockGroups
        ON StockItemStockGroups.StockGroupID = StockGroups.StockGroupID

        WHERE Orders.OrderDate BETWEEN '2013-01-01' AND '2017-12-31'
    ) AS nopvt
    PIVOT
    (
        SUM(Quantity)
        FOR YEAR IN ([2013], [2014], [2015], [2016], [2017]) 
    ) AS pvt
)

GO

SELECT *
FROM vwGroupQuantitySoldPerYear;

-- Problem 19
USE WideWorldImporters
GO

CREATE VIEW vwYearQuantitySoldPerGroup AS (
    SELECT Year, [Novelty Items], [Clothing], [Mugs], [T-Shirts], [Airline Novelties],
        [Computing Novelties], [USB Novelties], [Furry Footwear], [Toys], [Packaging Materials]
    FROM (
        SELECT StockGroups.StockGroupName,
        OrderLines.Quantity,
        Year(Orders.OrderDate) AS 'Year'
        FROM Sales.Orders

        INNER JOIN Sales.OrderLines
        ON Orders.OrderID = OrderLines.OrderID

        INNER JOIN Warehouse.StockItems
        ON OrderLines.StockItemID = StockItems.StockItemID

        INNER JOIN Warehouse.StockItemStockGroups
        ON StockItems.StockItemID = StockItemStockGroups.StockItemID

        INNER JOIN Warehouse.StockGroups
        ON StockItemStockGroups.StockGroupID = StockGroups.StockGroupID

        WHERE Orders.OrderDate BETWEEN '2013-01-01' AND '2017-12-31'
    ) AS nopvt
    PIVOT
    (
        SUM(Quantity)
        FOR StockGroupName IN ([Novelty Items], [Clothing], [Mugs], [T-Shirts], [Airline Novelties],
        [Computing Novelties], [USB Novelties], [Furry Footwear], [Toys], [Packaging Materials]) 
    ) AS pvt

)

GO

SELECT *
FROM vwYearQuantitySoldPerGroup
ORDER BY [Year];


-- Problem 20
USE WideWorldImporters
GO
DROP FUNCTION IF EXISTS total_order
GO
CREATE FUNCTION total_order( @OrderID INT )
RETURNS FLOAT AS
BEGIN
        DECLARE @total_order FLOAT
        SELECT @total_order = ISNULL(SUM(Quantity * UnitPrice * (1 + TaxRate/100)), 0)
        FROM Sales.OrderLines
        WHERE OrderID = @OrderID
        RETURN @total_order
END

GO
SELECT InvoiceID, dbo.total_order(OrderID) AS 'TotalOrder'
FROM Sales.Invoices

-- Problem 21
USE WideWorldImporters
GO
DROP SCHEMA IF EXISTS ods
GO
CREATE SCHEMA ods;
GO
CREATE TABLE ods.Orders(
    OrderID INT PRIMARY KEY
    OrderDate DATE,
    OrderTotal FLOAT,
    CustomerID INT
)


