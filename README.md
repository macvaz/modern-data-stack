# Modern data stack

Minimal example integrating docker images of the following Big Data open-source projects:

- **trino**: v425 - http://localhost:8080
- **MinIO**: v2023.08.23 - http://localhost:9000
- **HMS (Hive MetaStore)**: v3.1.3
- **Apache Spark**: v3.4.1
- **Apache Iceberg**: v1.6
- **Jupyter notebooks**: v1.0.0 - http://localhost:8000/tree/notebooks

There are also some technologies tested but **finally discarted** (have a look to the incompatibili  ties section):

- **Iceberg REST catalog**: v.16 - not compatible with trino when warehouse is NOT in AWS S3

Since the open-source big data ecosystem is vibrant, this **modern-data-stack is always evolving**. Currently, only the above projects are integrated but in a near future, other complementary and promising projects will be considered like:

- **OpenMetadata** (data catalog)
- **Apache Ranger** (data security)


## Installation

Start all docker containers with:

```bash
docker-compose up -d
```

### Initializing datalake

Since this repo is for teaching purposes, a `.env file` is provided. The access keys are totally disposable and not used in any system. For an easy setup, it's recommended to **keep that access keys to avoid changing the configuration files** ([metastore-site.xml](docker/hive-metastore/conf/metastore-site.xml) and [catalog/minio.properties](docker/trinodb/conf/catalog/minio.properties)) where this keys are used. 

To provision access keys and creating the bucket in the MinIO server, just type:

```bash
docker-compose exec minio bash /opt/bin/init_datalake.sh
bash libs/download_s3_jars.sh
```
### Testing installation

Since all Spark source base runs on JVM platform, using spark-shell (instead of pyspark) is recommended for troubleshooting installation errors. This avoid confusing wrapped Python-style errors of JVM components.  

To test the installation, just start a scala-based spark shell, just type:

```bash
docker-compose exec spark spark-shell 
```

Just past this code in the shell in order to test the connection between Spark and iceberg REST metastore:

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
df.writeTo("nyc.test").create()
```

No errors should be raised. For debugging purposes, log files are always a good place to look at. Log files are stored in spark docker container and can checked like this:

```bash
docker-compose exec spark bash
ls -al /home/iceberg/spark-events/*
```

## Using Jupyter notebooks

Once installation is properly set up, using **jupyter notebooks** is much more covenient than CLI tools. Since python kernel is distributed in the spark-iceberg docker image, **all coding examples are developed in python**. 

**Open the notebook** called [Testing Iceberg](http://localhost:8000/notebooks/notebooks/Testing%20Iceberg.ipynb). Iceberg project mantains a very good [quick start guide](https://iceberg.apache.org/spark-quickstart/#creating-a-table) that complements this notebook.

The `iceberg` catalog is configured in [this file](docker/spark-iceberg/conf/spark-defaults.iceberg.conf) and passed to the spark container as the spark-defaults.conf file. This file sets Iceberg as default table format for this catalog. It alse sets Iceberg REST catalog as metastore for the catalog.

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

Using trino with the **iceberg catalog** stores all metadata in Iceberg TEST data catalog. This Big Data table (**iceberg.nyc.sales**) can be read using both trino SQL and by native Big Data technologies like Apache Spark. 

```sql
CREATE SCHEMA IF NOT EXISTS minio.nyc WITH (location = 's3a://warehouse/nyc');

CREATE TABLE IF NOT EXISTS minio.nyc.sales (
  productcategoryname VARCHAR,
  productsubcategoryname VARCHAR,
  productname VARCHAR,
  customerName VARCHAR,
  salesTerritoryCountry VARCHAR,
  salesOrderNumber VARCHAR,
  orderQuantity INTEGER
);

select * from minio.nyc.sales;
```

## Compatibility issues

### Apache Hive MetaStore

**[2023/08/26]** Since Apache Hive team publishes in [docker hub](https://hub.docker.com/r/apache/hive/tags) linux/amd64 images for both v3 and v4 (alpha and beta prereleases), I decided to test them:
 * Both alpha v4 and beta v4 prereleases work perfectly well but Trino v425 is only compatible with [Hive Metastore Thrift API v3.1](https://github.com/trinodb/trino/blob/39af728fa5e474d5537ede364f7599c941541f2f/pom.xml#L1393). In real life usage, this produces some incompatibility errors when using trino that can be easily reproduced using [hive4 branch](https://github.com/macvaz/modern_data_stack/tree/hive4) of this repo.

 * [Official Hive 3.1.3 docker image](https://hub.docker.com/layers/apache/hive/3.1.3/images/sha256-d3d2b8dff7c223b4a024a0393e5c89b1d6cb413e91d740526aebf4e6ecd8f75e?context=explore) does not start, showing database initializacions errors. Consecuently, I wrote my own [Dockerfile](docker/hive-metastore/Dockerfile) for installing HMS 3.1.3.

### Apache Iceberg REST catalog

**[2023/09/03]** Is compatible with [several metastore catalogs](https://iceberg.apache.org/concepts/catalog/) apart from Hive Metastore like REST Catalog and JDBC Catalog. However [trino does not have full suport](https://trino.io/docs/current/connector/metastores.html) for them, making imposible to crate views and materialized views with REST Catalog and JDBC Catalog. Consequently, modern-data-stack is still based on Hive MetaStore (HMS) until this limitations are overcame.

Additionally, since trino does not allow to redefine the S3 endpoint when using the REST catalog, trino will always try to connect to AWS S3 public cloud and not to local MinIO. Iceberg REST catalog is not an option currently for this PoC.

**Spark works perfectly well with Iceberg REST catalog** and the spark-defaults needed are is [this file](docker/spark-iceberg/conf/spark-defaults.iceberg.conf). However, the **selected metastore for the modern data stack is HMS** mainly for compatibility issues (specially with trino)

## Usefull links

* https://iceberg.apache.org/spark-quickstart/
* https://tabular.io/blog/docker-spark-and-iceberg/
* https://blog.min.io/manage-iceberg-tables-with-spark/
* https://blog.min.io/iceberg-acid-transactions/
* https://tabular.io/blog/rest-catalog-docker/
* https://blog.min.io/lakehouse-architecture-iceberg-minio/
* https://tabular.io/blog/iceberg-fileio/?ref=blog.min.io
* https://tabular.io/guides-and-papers/
