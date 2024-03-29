-- Olympics dataset 
-- https://techtfq.com/blog/practice-writing-sql-queries-using-real-dataset#google_vignette=

-- QUERY 4
with participants as (
	select games, count(distinct(noc)) as country_participation
	from hms.data_db.olympic_events
	group by games
)

select distinct 
	concat(
		first_value(games) over (order by country_participation), 
		' - ', 
		format_number(first_value(country_participation) over (order by country_participation))
	) as less_countries,
	concat(
		first_value(games) over (order by country_participation desc),
		' - ',
		format_number(first_value(country_participation) over (order by country_participation desc))
	) as more_countries
from participants