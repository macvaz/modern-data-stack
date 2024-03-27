# Modern data stack

Minimal example integrating different open-source technologies in order to create a working **open data lakehouse** solution based on **Apache Iceberg**. 

Core technologies used are:

| Component | Description | Version |  URL  |
| --------- | ----------- | ------- | ----- |
| Trino     | Federated query engine | v443 | http://localhost:8080
| MinIO     | Object store   |  v2023.08.23  | http://localhost:9000
| Hive MestaStore (HMS) | Metadata respository |    v.3.1.3
| Apache Spark | Distributed computation engine | v3.4.1 | 
| Apache Iceberg | Analytics table open format | v1.6
| Jupyter notebook | Web-based computational documents | v1.0.0 | http://localhost:8000/tree

### Code structure

All software **components are distributed based on docker images**. There is a docker-compose file that automates the deployment of all components easily. All components' configuration files are inside [docker directory](docker).

In the [notebooks folder](notebooks), there is a **Jupyter notebook showing a simplified Spark-based ETL batch job** that generates data products stored in MinIO object storage. All data products are registered into the Hive MetaStore (HMS) service as Apache Iceberg tables. 

Both Spark (for data creation) anf Trino (for interactive data exposition) use the lakehouse capabilities through Hive metastore (HMS), creating a seamless integration of ETL workloads and interactive querying.

Inside [datasets folder](datasets) are a small-sized files with samples of the datasets used.

### Other technologies to be considered

Since only the above technologies are successfully integrated, other complementary and promising projects will be considered in a future like:

| Component | Description | 
| --------- | ----------- |
| OpenMetadata     | Metadata management (catalog, lineage) | 
| Open policy agent (OPA)     | Centralized access policy repository and enforcing system   |  
| DBT | SQL-first transformation workflow 

## Installation

Start all docker containers with:

```bash
docker-compose up -d
```

### Initializing lakehouse

Since this repo is for teaching purposes, a `.env file` is provided. The provided access keys are totally disposable and not used in any system. For an easy setup, it's recommended to **keep that access keys unaltered to avoid changing the following configuration files**:
* [HMS metastore-site.xml](docker/hive-metastore/conf/metastore-site.xml)
* [spark-defaults.conf](docker/spark-iceberg/conf/spark-defaults.conf)
* [trino catalog](docker/trinodb/conf/catalog/hms.properties)

Initializing our open lakehouse requires invoking MinIO object store API. To provision in MinIO a object store `access key`, `access secret` and a `S3 bucket` called `lakehouse` in the MinIO server, just type:

```bash
docker-compose exec minio bash /opt/bin/init_lakehouse.sh
```

## Using Spark via Jupyter notebooks

Since python kernel is distributed in the spark-iceberg docker image, **all coding examples are developed in python and SQL**. No scala is used to simplify reading the code. 

**Open the notebook** called [Spark ETL](http://localhost:8000/notebooks/Spark_ETL.ipynb) and execute it. The following database objects will be created in the lakehouse:
 - `sales_db` database with `sales_summary` table
 - `trip_db` database with `trips` table 

The Spark `hms catalog` is configured in [this file](docker/spark-iceberg/conf/spark-defaults.conf) and passed to the spark container as the spark-defaults.conf file. This file sets Iceberg as default table format for this catalog. It also sets HMS as catalog's metastore.

## Using Trino

trino client is installed in the trino container. To access trino using a CLI, just connect to it:

```bash
docker-compose exec trino trino
```

A very convenient way of connecting to trino is through its JDBC API. Using a generic JDBC client like [DBeaver Community](https://dbeaver.io/) is the recommended way of using trino.

Using trino with the **iceberg connector** sets the default table format to Iceberg. Creating a `trino catalog` called **hms**, using Iceberg connector and pointing to HMS, is done in  [this file](docker/trinodb/conf/catalog/hms.properties). 

Iceberg tables created from Spark in HMS can be used seamlessly from trino and vice-versa.  

```sql
CREATE SCHEMA IF NOT EXISTS hms.trip_trino_db WITH (location = 's3a://lakehouse/warehouse/trip_trino_db');

CREATE TABLE IF NOT EXISTS hms.trip_trino_db.sales (
  productcategoryname VARCHAR,
  productsubcategoryname VARCHAR,
  productname VARCHAR,
  customerName VARCHAR,
  salesTerritoryCountry VARCHAR,
  salesOrderNumber VARCHAR,
  orderQuantity INTEGER
);

select * from hms.trip_trino_db.sales;
```

Databases and tables registrered in Hive MetaStore are shared by different technologies. In the next snippet, a new table created from Trino is added to a database created by Spark:

```sql
CREATE TABLE IF NOT EXISTS hms.trip_db.sales_from_trino (
  productcategoryname VARCHAR,
  productsubcategoryname VARCHAR,
  productname VARCHAR,
  customerName VARCHAR,
  salesTerritoryCountry VARCHAR,
  salesOrderNumber VARCHAR,
  orderQuantity INTEGER
);

select * from hms.trip_db.sales_from_trino ;
```

## Useful links

* https://iceberg.apache.org/spark-quickstart/
* https://tabular.io/blog/docker-spark-and-iceberg/
* https://blog.min.io/manage-iceberg-tables-with-spark/
* https://blog.min.io/iceberg-acid-transactions/
* https://tabular.io/blog/rest-catalog-docker/
* https://blog.min.io/lakehouse-architecture-iceberg-minio/
* https://tabular.io/blog/iceberg-fileio/?ref=blog.min.io
* https://tabular.io/guides-and-papers/
* https://www.datamesh-architecture.com/tech-stacks/minio-trino
* https://dev.to/alexmercedcoder/configuring-apache-spark-for-apache-iceberg-2d41
* https://iceberg.apache.org/docs/latest/configuration/#catalog-properties

