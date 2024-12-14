-------------------------------------------------
-- Pivot
-------------------------------------------------
-- Pivot requires a categorical columna + a metric column
-- It can be reshaped into 1 column per category value

-- Source table
SELECT orderdate, salesterritorycountry, sum(totalproductcost) as total_sales
FROM hms.data_db.sales_summary
group by salesterritorycountry, orderdate  

-- Manual pivot using one case when per category value
SELECT 
	orderdate, 
	sum(case when salesterritorycountry = 'Australia' then totalproductcost else 0 end) as Australia,
	sum(case when salesterritorycountry = 'Germany' then totalproductcost else 0 end) as Germany,
	sum(case when salesterritorycountry = 'France' then totalproductcost else 0 end) as France,
	sum(case when salesterritorycountry = 'United States' then totalproductcost else 0 end) as United_States,
	sum(case when salesterritorycountry = 'Canada' then totalproductcost else 0 end) as Canada
FROM hms.data_db.sales_summary
group by salesterritorycountry, orderdate  

-- Trino does not support PIVOT keyword 
-- Next SQL code is from AWS Redshift that supports PIVOT

SELECT *
FROM (SELECT category_names, metric FROM part) PIVOT (
    AVG(metric) FOR partname IN ('category1', 'category2', 'category3')
);


-------------------------------------------------
-- Unpivot
-------------------------------------------------
-- unpivot requires several metrics columns
-- It can be reshaped into 2 columns, 1 categorical (name category in sample SQL) + 1 metrics (called metric)
-- Metrics columns types should be the same

SELECT *
FROM (SELECT metrics1, metrics2, metrics3 FROM table) UNPIVOT (
    metric FOR category IN (metrics1, metrics2, metrics3)
);