-- SELECT
--   DATEPART(YEAR, h.OrderDate) AS Year,
--   DATEPART(MONTH, h.OrderDate) AS Month,
--   SUM(d.LineTotal) AS TotalSales
-- FROM SalesLT.SalesOrderHeader h
-- JOIN SalesLT.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
-- WHERE h.OrderDate BETWEEN '2000-01-01' AND '2025-12-31'
-- GROUP BY DATEPART(YEAR,h.OrderDate), DATEPART(MONTH,h.OrderDate);

-- SELECT
--   YEAR(h.OrderDate) AS Year,
--   MONTH(h.OrderDate) AS Month,
--   SUM(d.LineTotal) AS Revenue,
--   SUM(p.StandardCost * d.OrderQty) AS COGS,
--   CASE WHEN SUM(d.LineTotal) = 0 THEN NULL
--        ELSE (SUM(d.LineTotal) - SUM(p.StandardCost * d.OrderQty)) * 100.0 / SUM(d.LineTotal)
--   END AS GrossMarginPct
-- FROM SalesLT.SalesOrderHeader h
-- JOIN SalesLT.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
-- JOIN SalesLT.Product p ON d.ProductID = p.ProductID
-- WHERE h.OrderDate BETWEEN '2000-01-01' AND '2025-12-31'
-- GROUP BY YEAR(h.OrderDate), MONTH(h.OrderDate);

WITH OrderTotals AS (
  SELECT d.SalesOrderID, SUM(d.LineTotal) AS OrderTotal, MIN(h.OrderDate) AS OrderDate
  FROM SalesLT.SalesOrderHeader h
  JOIN SalesLT.SalesOrderDetail d ON h.SalesOrderID = d.SalesOrderID
  WHERE h.OrderDate BETWEEN '2000-01-01' AND '2025-12-31'
  GROUP BY d.SalesOrderID
)
SELECT YEAR(OrderDate) AS Year, MONTH(OrderDate) AS Month,
       AVG(OrderTotal) AS AOV, COUNT(*) AS OrdersCount
FROM OrderTotals
GROUP BY YEAR(OrderDate), MONTH(OrderDate);

-- SELECT YEAR(h.ShipDate) AS Year, MONTH(h.ShipDate) AS Month,
--        SUM(CASE WHEN h.ShipDate <= h.DueDate THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS OnTimePct,
--        COUNT(*) AS TotalShipments
-- FROM SalesLT.SalesOrderHeader h
-- WHERE h.ShipDate IS NOT NULL AND h.ShipDate BETWEEN '2000-01-01' AND '2025-12-31'
-- GROUP BY YEAR(h.ShipDate), MONTH(h.ShipDate);

WITH MonthlyProductSales AS (
    SELECT 
        DATEPART(YEAR, h.OrderDate) AS Año,
        DATEPART(MONTH, h.OrderDate) AS Mes,
        p.Name AS Producto,
        SUM(d.LineTotal) AS VentasProducto,
        RANK() OVER (
            PARTITION BY DATEPART(YEAR, h.OrderDate),
                         DATEPART(MONTH, h.OrderDate)
            ORDER BY SUM(d.LineTotal) DESC
        ) AS RankingMensual
    FROM SalesLT.SalesOrderHeader h
    JOIN SalesLT.SalesOrderDetail d 
        ON h.SalesOrderID = d.SalesOrderID
    JOIN SalesLT.Product p
        ON d.ProductID = p.ProductID
    GROUP BY 
        DATEPART(YEAR, h.OrderDate),
        DATEPART(MONTH, h.OrderDate),
        p.Name
)
SELECT *
FROM MonthlyProductSales
WHERE RankingMensual <= 5
ORDER BY Año, Mes, RankingMensual;
