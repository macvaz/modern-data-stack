-- https://techtfq.com/blog/practice-writing-sql-queries-using-real-dataset#google_vignette=

-- Window functions can be sean as derivative fields 

----------------------------------------
-- Cumulative sums requires unique identifiers to order. 
-- If no pks, can be simulated with row_number window function

-- Using subqueries
select productsubcategoryname, productname, orderdate , totalproductcost, sum(totalproductcost) over (order by id) as cumulative_sum
from (
	SELECT 
	     productsubcategoryname, productname, orderdate , totalproductcost, 
	     row_number() over (order by orderdate) as id
	FROM hms.sales_db.sales_summary
)

-- Using CTEs
WITH sales_with_id AS (
	SELECT 
	     productsubcategoryname, productname, totalproductcost, 
	     row_number() over (order by orderdate) as id
	FROM hms.sales_db.sales_summary
)

SELECT
	id, productsubcategoryname, productname, totalproductcost, 
	sum(totalproductcost) over (order by id) as cumulative_sum
from sales_with_id;

-- Running total requires no duplicates in order by column
SELECT productsubcategoryname, productname, orderdate , salesordernumber, totalproductcost, sum(totalproductcost) over (order by orderdate) as running_cost
FROM hms.sales_db.sales_summary;


SELECT 
	productname, salesordernumber, totalproductcost, orderdate, 
	dense_rank() over (partition by productname order by salesordernumber) as position,
	sum(totalproductcost) over (partition by productname order by salesordernumber) as cumulative_cost
FROM hms.sales_db.sales_summary
order by salesordernumber  


-- REMOVED DUPLICATES
WITH RANKED_SALES AS (
    select
		productname, orderdate, totalproductcost,
		row_number() over (partition by orderdate order by orderdate) as ranking
	from hms.sales_db.sales_summary 
)

select
	productname, orderdate, totalproductcost, 
	dense_rank() over (partition by productname order by orderdate) as ranking,
	sum(totalproductcost) over (partition by productname order by orderdate) as cumulative_total
from 
RANKED_SALES
where ranking = 1;
