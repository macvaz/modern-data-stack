-- Olympics dataset 
-- https://techtfq.com/blog/practice-writing-sql-queries-using-real-dataset#google_vignette=

use hms.data_db;

-- QUERY 1
select count(distinct games) as total_olympic_games 
from hms.data_db.olympic_events ;

-- QUERY 2
select distinct year, season, city
from data_db.olympic_events
order by year;

-- QUERY 3
select games, count(distinct noc)
from data_db.olympic_events
group by games
order by games

-- QUERY 4
with participants as (
	select games, count(distinct(noc)) as country_participation
	from hms.data_db.olympic_events
	group by games
)
select distinct 
	concat(
		first_value(games) over (order by country_participation)
		,' - '
		,format_number(first_value(country_participation) over (order by country_participation))
	) as less_countries,
	concat(
		first_value(games) over (order by country_participation desc)
		,' - '
		,format_number(first_value(country_participation) over (order by country_participation desc))
	) as more_countries
from participants

-- QUERY 5
-- Option 1: Using group by + having + 1 CTE
with all_games as (
		select distinct games, noc
		from data_db.olympic_events
	)
select noc, count(*) as total_participated_games
from all_games
group by 1
having count() = (select count(distinct games) from all_games)

-- Option 2: Using where + 3 CTE
with all_games as (
		select distinct games, noc
		from data_db.olympic_events
	),
	all_games_count as (
		select count(distinct games) as games_count
		from all_games
	),
	games_count as (
		select noc, count(*) as games_count
		from all_games
		group by 1
	)
select gc.*
from games_count gc, all_games_count agc
where gc.games_count = agc.games_count

-- Alternative 3: Using join + 3 CTEs
with all_games as (
		select distinct games, noc
		from data_db.olympic_events
	),
	all_games_count as (
		select count(distinct games) as games_count
		from all_games
	),
	games_count as (
		select noc, count(*) as games_count
		from all_games
		group by 1
	)
select gc.*
from games_count gc
join all_games_count agc on gc.games_count = agc.games_count

-- QUERY 6
with all_games_count as (
		select count(distinct games) as games_count
		from hms.data_db.olympic_events
		where season = 'Summer'
),   
	sport_count as (
		select sport, count (distinct games) as games_count
		from hms.data_db.olympic_events
		where season = 'Summer'
		group by sport
)
select sport 
from sport_count sc
join all_games_count agc on sc.games_count = agc.games_countdd

-- QUERY 7
with all_sports as (
		select distinct sport, games
		from hms.data_db.olympic_events
)
select sport
from all_sports
group by sport
having count(games) = 1
order by 1

-- QUERY 8
with sports as (
		select distinct games, sport
		from hms.data_db.olympic_events 
)
select games, count(sport) as num_sports
from sports
group by games
order by 2 desc

-- QUERY 18
with medals as (
	select 
		case when medal = 'Gold' then 1 else 0 end as gold_medal,
		case when medal = 'Silver' then 1 else 0 end as silver_medal,
		case when medal = 'Bronze' then 1 else 0 end as bronze_medal,
		region
	from hms.data_db.olympic_events e
	join hms.data_db.olympic_regions r on r.noc = e.noc
),
	medals_count as (
	select 
		region, 
		sum(gold_medal) as gold_count,
		sum(silver_medal) as silver_count,
		sum(bronze_medal) as bronze_count
	from medals
	group by region
)
select *
from medals_count
where gold_count = 0 and (silver_count > 0 or bronze_count > 0)
order by 3,4

-- QUERY 19
--Alternative 1: With ctes + case when + sum
with india_events as (
		select games, sport, case when medal in ('Gold', 'Silver', 'Bronze') then 1 else 0 end as medal_count
		from hms.data_db.olympic_events 
		where noc = 'IND'
), 
	india_medals as (
		select sport, sum(medal_count) as total_medals
		from india_events
		group by sport
	)
select *
from india_medals
order by 2 desc
limit 1

-- Alternative 2: Using count and != (instead of case when)
select sport, count(medal)
from hms.data_db.olympic_events 
where noc = 'IND' and medal != 'NA'
group by 1
order by 2 desc
limit 1
