# Modern data stack

Minimal example integrating docker images of the following Big Data open-source projects:

- **trino**: v425
- **MinIO**: v2023.08.23 - http://localhost:9000
- **HMS (Hive MetaStore)**: v3.1.3
- **Apache Spark**: v3.4.1
- **Apache Iceberg**: v1.6
- **Jupyter notebooks**: v1.0.0 - http://localhost:8000/tree/notebooks

Since the open-source big data ecosystem is vibrant, this **modern-data-stack is always evolving**. Currently, only the above projects are integrated but in a near future, other complementary and promising projects will be considered like:

- **OpenMetadata** (data catalog)
- **Apache Ranger** (data security)


## Installation

Start all docker containers with:

```bash
docker-compose up -d
```

Connect to `http://localhost:9000` using the `MINIO_USERNAME` and `MINIO_USER_PASSWORD` provided via `.env file`. **Create a pair of `access key` and `secret key`** in MinIO. This credentials should be added to [metastore-site.xml](docker/hive-metastore/conf/metastore-site.xml) and [catalog/minio.properties](docker/trino/conf/catalog/minio.properties) in order to integrate HMS, MinIO and trino. Since this repo is for teaching purposes, **it's recommended to create the same keys in MinIO to avoid changing the configuration files**.

Create the buckets in the MinIO server:

```bash
docker-compose exec minio bash /opt/bin/init_datalake.sh
```

### Testing Spark + Iceberg installation

Since all Spark source base runs on JVM platform, using spark-shell (instead of pyspark) is recommended for troubleshooting installation errors. This avoid confusing wrapped Python-style errors of JVM components.  

To start a scala-bases spark shell, just type:

```bash
docker-compose exec spark spark-shell 
```

Just paste this code in the shell in order to test the connection between Spark and iceberg REST metastore:


```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val schema = StructType( Array(
    StructField("vendor_id", LongType,true),
    StructField("trip_id", LongType,true),
    StructField("trip_distance", FloatType,true),
    StructField("fare_amount", DoubleType,true),
    StructField("store_and_fwd_flag", StringType,true)
))

val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
df.writeTo("iceberg.nyc.taxis").create()
```

For debugging purposes, log files are always a good place to look at. Log files are stored in spark docker container and can checked like this:

```bash
docker-compose exec spark bash
ls -al /home/iceberg/spark-events/*
```

## Using Jupyter notebooks

Once installation is properly set up, using **jupyter notebooks** is much more covenient than CLI tools. Since python kernel is distributed in the spark-iceberg docker image, all coding examples are developed in python. 

To connect to the jupyter notebook environment, just connect to the url **http://localhost:8000/tree/notebooks**. Open the notebook called **"Testing Iceberg"**.Iceberg project mantains a very good [quick start guide](https://iceberg.apache.org/spark-quickstart/#creating-a-table). Next examples are based on the official documentation of the project.

The `iceberg` catalog is configured in [this file](docker/spark-iceberg/conf/spark-defaults.iceberg.conf) and passed to the spark container as the spark-defaults.conf file. Changes This file sets Iceberg as default table format for this catalog. It alse sets Icebert REST catalog as metastore for the catalog.

If everything is properly setup, a new namespace (a.k.a database) called `nyc` will be created in the Iceberg REST catalog. This namespace contains also a table called `taxis`. This table is created using iceberg table format since `iceberg` catalog is configured to use `iceberg` by default.

The REST catalog exposed a REST API than can also be invoked to retrieve the metastore status:
* http://localhost:8181/v1/namespaces
* http://localhost:8181/v1/namespaces/nyc
* http://localhost:8181/v1/namespaces/nyc/tables


## Using Trino

trino client is installed in the trino container:
```bash
docker-compose exec trino trino
```

Using trino with the **minio catalog** stores all metadata in Hive MetaStore (HMS) data catalog. This Big Data table (**minio.sales.sales**) can be read using both trino SQL and by native Big Data technologies like Apache Spark. 

```sql
./trino
CREATE SCHEMA IF NOT EXISTS minio.sales WITH (location = 's3a://minio-dlk/sales');

CREATE TABLE IF NOT EXISTS minio.sales.sales (
  productcategoryname VARCHAR,
  productsubcategoryname VARCHAR,
  productname VARCHAR,
  customerName VARCHAR,
  salesTerritoryCountry VARCHAR,
  salesOrderNumber VARCHAR,
  orderQuantity INTEGER
)
WITH (
  external_location = 's3a://minio-dlk/sales/',
  format = 'PARQUET'
);

select * from minio.sales.sales;
```

## Compatibility issues

### Apache Hive MetaStore

**[2023/08/26]** Since Apache Hive team publishes in [docker hub](https://hub.docker.com/r/apache/hive/tags) linux/amd64 images for both v3 and v4 (alpha and beta prereleases), I decided to test them:
 * Both alpha v4 and beta v4 prereleases work perfectly well but Trino v425 is only compatible with [Hive Metastore Thrift API v3.1](https://github.com/trinodb/trino/blob/39af728fa5e474d5537ede364f7599c941541f2f/pom.xml#L1393). In real life usage, this produces some incompatibility errors when using trino that can be easily reproduced using [hive4 branch](https://github.com/macvaz/modern_data_stack/tree/hive4) of this repo.

 * [Official Hive 3.1.3 docker image](https://hub.docker.com/layers/apache/hive/3.1.3/images/sha256-d3d2b8dff7c223b4a024a0393e5c89b1d6cb413e91d740526aebf4e6ecd8f75e?context=explore) does not start, showing database initializacions errors. Consecuently, I wrote the Dockerfile for installing HMS 3.1.3.

### Apache Iceberg

Is compatible with [several metastore catalogs](https://iceberg.apache.org/concepts/catalog/) apart from Hive Metastore like REST Catalog and JDBC Catalog. However [trino does not have full suport](https://trino.io/docs/current/connector/metastores.html) for them, making imposible to crate views and materialized views with REST Catalog and JDBC Catalog. Consequently, modern-data-stack is still based on Hive MetaStore (HMS) until this limitations are overcame.