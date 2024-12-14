create table data_db.suba_observations (
	datapoint_id VARCHAR,
	variable_id VARCHAR,
	module_id INTEGER,
	taxonomy_name VARCHAR,
	open_columns MAP<VARCHAR, VARCHAR>
);

create table data_db.suba_observations_2_columns (
	datapoint_id VARCHAR,
	variable_id VARCHAR,
	module_id INTEGER,
	taxonomy_name VARCHAR,
	open_columns_keys VARCHAR,
	open_columns_values VARCHAR
) 
