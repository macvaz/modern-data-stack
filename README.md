# Modern data stack

Minimal example integrating docker images of the following Big Data open-source projects:

```bash
  - trino: v425
  - MinIO: v2023.08.23
  - HMS (Hive MetaStore): v3.1.3
  - Apache Spark: v3.4.1
  - Apache Iceberg: v1.6
  - Jupyter notebooks: v1.0.0 (with iJava and Python kernels)
```

Since the open-source big data ecosystem is vibrant, this **modern-data-stack is always evolving**. Currently, only the above projects are integrated but in a near future, other complementary and promising projects will be considered like:

```bash
  - OpenMetadata (data catalog)
  - Apache Ranger (data security)
```
## Installation

Start all docker containers with:

```bash
docker-compose up -d
```

Connect to `http://localhost:9000` using the `MINIO_USERNAME` and `MINIO_USER_PASSWORD` provided via `.env file`. **Create a pair of `access key` and `secret key`** in MinIO. This credentials should be added to [metastore-site.xml](docker/hive-metastore/conf/metastore-site.xml) and [catalog/minio.properties](docker/trino/conf/catalog/minio.properties) in order to integrate HMS, MinIO and trino. Since this repo is for teaching purposes, **it's recommended to create the same keys in MinIO to avoid changing the configuration files**.

The basic autentication scheme in MinIO for buckets is based on S3 access tokens. Exploring other authentication methods is out of scope of this repo. **The keys used in this repo are disposable, created adhoc in a one-off VM**. I decided to hardcode them in order to keep things simple for the reader. 

Create the buckets in the MinIO server:

```bash
docker-compose exec minio bash /opt/bin/init_datalake.sh
```

## Using Spark

```bash
docker-compose exec spark spark-shell #Scala shell
docker-compose exec spark pyspark #Python shell
```

## Using Jupyter notebooks

Connect using a web browser to: http://localhost:8000/tree

There is only kernels for python, so pyspark is the prefered way to use spark.

## Using Trino

trino client is installed in the trino container:
```bash
docker-compose exec trino trino
```

Using trino with the **minio catalog** stores all metadata in Hive MetaStore (HMS) data catalog. This Big Data table (**minio.sales.sales**) can be read using both trino SQL and by native Big Data technologies like Apache Spark. 

```bash
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