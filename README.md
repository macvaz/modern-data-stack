# Modern data stack

Minimal example integrating different open-source technologies in order to create a working **open data lakehouse** solution based on **Apache Iceberg**. Core technologies used are:

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

### Initializing datalake

Since this repo is for teaching purposes, a `.env file` is provided. The provided access keys are totally disposable and not used in any system. For an easy setup, it's recommended to **keep that access keys unaltered to avoid changing the following configuration files**:
* [HMS metastore-site.xml](docker/hive-metastore/conf/metastore-site.xml)
* [spark-defaults.conf](docker/spark-iceberg/conf/spark-defaults.conf)
* [trino catalog](docker/trinodb/conf/catalog/hms.properties)

To provision access keys and creating the bucket in the MinIO server, just type:

```bash
docker-compose exec minio bash /opt/bin/init_datalake.sh
```

### Testing installation

Since all Spark source base runs on JVM platform, using spark-shell (instead of pyspark) is recommended for troubleshooting installation errors. This avoid confusing wrapped Python-style errors of JVM components.  

To test the installation, just start a scala-based spark shell, just type:

```bash
docker-compose exec spark spark-shell 
```

Just past this code in the shell in order to test the connection between Spark, Iceberg runtime and Hive metastore:

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

spark.sql("CREATE DATABASE IF NOT EXISTS nyc300 LOCATION 's3a://warehouse/nyc300';")

val schema = StructType( Array(
    StructField("vendor_id", LongType,true),
    StructField("trip_id", LongType,true),
    StructField("trip_distance", FloatType,true),
    StructField("fare_amount", DoubleType,true),
    StructField("store_and_fwd_flag", StringType,true)
))

val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
df.writeTo("hms.nyc300.test").create()
```

No errors should be raised. For debugging purposes, log files are always a good place to look at. Log files are stored in spark docker container and can checked like this:

```bash
docker-compose exec spark bash
ls -al /home/iceberg/spark-events/*
```

## Using Jupyter notebooks

Once installation is properly set up, using **jupyter notebooks** is much more covenient than CLI tools. Since python kernel is distributed in the spark-iceberg docker image, **all coding examples are developed in python and SQL**. No scala is used to simplify reading the code. 

**Open the notebook** called [Testing Iceberg tables](http://localhost:8000/notebooks/Testing%20Iceberg%20tables.ipynb). This notebook is totally inspired by excelent Iceberg [quick start guide](https://iceberg.apache.org/spark-quickstart/#creating-a-table).

The Spark `hms catalog` is configured in [this file](docker/spark-iceberg/conf/spark-defaults.conf) and passed to the spark container as the spark-defaults.conf file. This file sets Iceberg as default table format for this catalog. It also sets HMS as catalog's metastore.

If everything is properly setup, a new namespace (a.k.a database) called `nyc100` will be created in HMS executing the first notebook cell. All tables created in this notebook, using both python spark API and SQL, uses Iceberg table format underneath due to the spark-defaults defined [here](docker/spark-iceberg/conf/spark-defaults.conf).

## Using Trino

trino client is installed in the trino container. Just connect to it:
```bash
docker-compose exec trino trino
```

Using trino with the **iceberg connector** sets the default table format to Iceberg. Creating a `trino catalog` called **hms**, using Iceberg connector and poiting to HMS, is done in  [this file](docker/trinodb/conf/catalog/hms.properties). Iceberg tables created from Spark in HMS can be used seamlessly from trino and vice-versa.  

```sql
CREATE SCHEMA IF NOT EXISTS hms.nyc200 WITH (location = 's3a://warehouse/nyc200');

CREATE TABLE IF NOT EXISTS hms.nyc200.sales_from_trino (
  productcategoryname VARCHAR,
  productsubcategoryname VARCHAR,
  productname VARCHAR,
  customerName VARCHAR,
  salesTerritoryCountry VARCHAR,
  salesOrderNumber VARCHAR,
  orderQuantity INTEGER
);

select * from hms.nyc200.sales_from_trino;
```

## Compatibility issues

### Apache Hive MetaStore

**[2023/08/26]** Since Apache Hive team publishes in [docker hub](https://hub.docker.com/r/apache/hive/tags) linux/amd64 images for both v3 and v4 (alpha and beta prereleases), I decided to test them:
 * Both alpha v4 and beta v4 prereleases work perfectly well but Trino v425 is only compatible with [Hive Metastore Thrift API v3.1](https://github.com/trinodb/trino/blob/39af728fa5e474d5537ede364f7599c941541f2f/pom.xml#L1393). In real life usage, this produces some incompatibility errors when using trino that can be easily reproduced using [hive4 branch](https://github.com/macvaz/modern_data_stack/tree/hive4) of this repo.

 * [Official Hive 3.1.3 docker image](https://hub.docker.com/layers/apache/hive/3.1.3/images/sha256-d3d2b8dff7c223b4a024a0393e5c89b1d6cb413e91d740526aebf4e6ecd8f75e?context=explore) does not start, showing database initializacions errors. Consecuently, I wrote my own [Dockerfile](docker/hive-metastore/Dockerfile) for installing HMS 3.1.3.

### Apache Iceberg REST catalog

**[2023/09/03]** Trino is compatible with [several metastore catalogs](https://iceberg.apache.org/concepts/catalog/) apart from Hive Metastore like REST Catalog and JDBC Catalog. However [trino does not have full suport](https://trino.io/docs/current/connector/metastores.html) for them, making imposible to crate views and materialized views with REST Catalog and JDBC Catalog. Consequently, modern-data-stack is still based on Hive MetaStore (HMS) until this limitations are overcame.

Additionally, since trino does not allow to redefine the S3 endpoint when using the REST catalog, trino will always try to connect to AWS S3 public cloud and not to local MinIO. Iceberg REST catalog is not an option currently for this PoC.

**Spark works perfectly well with Iceberg REST catalog** and the spark-defaults needed are is [this file](docker/spark-iceberg/conf/spark-defaults.conf). However, the **selected metastore is HMS** mainly for compatibility issues (specially with trino)

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

