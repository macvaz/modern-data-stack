# Modern data stack

Minimal example integrating in Docker the following components:

```bash
  - Trino: v425
  - MinIO: v2023.08.23
  - HMS (Apache Hive standalone metastore): v3.0.0
```

## Compatibility issues

*[Note of 2023/08/26]* Trino does not work with Apache Hive 4 (beta1). HMS v4.0.0-beta-1 starts perfectly but some incompatibility isues arise when using trino. 

Trino uses [version 3.1](https://github.com/trinodb/trino/blob/39af728fa5e474d5537ede364f7599c941541f2f/pom.xml#L1393) of the Metastore Thift API and HMS 4.0 changes the Thrift interface. The practical results integrating latest versions of Trino and HMS can be checked at [hive4 branch](https://github.com/macvaz/modern_data_stack/tree/hive4)

I started working with HMS 3.1.3 but current official [Apache Hive docker image](https://hub.docker.com/layers/apache/hive/3.1.3/images/sha256-d3d2b8dff7c223b4a024a0393e5c89b1d6cb413e91d740526aebf4e6ecd8f75e?context=explore) does not start properly showing some database initializacions errors. Consecuently, this repo uses an old (v3.0.0) and unoffical docker container of HMS coming from https://github.com/bitsondatadev/hive-metastore

## Installation and Setup

Install [s3cmd](https://s3tools.org/s3cmd) with:

```bash
sudo apt update
sudo apt install -y \
    s3cmd \
    openjdk-11-jre-headless  # Needed for trino-cli
```

Pull and run all services with:

```bash
docker-compose up
```

Connect to `http://localhost:9000` using the `MINIO_USERNAME` and `MINIO_USER_PASSWORD` provided to docker-compose via `.env file` and **create a pair of `access key` and `secret key`**. This credentials should be added to [metastore-site.xml](conf/hive-metastore/metastore-site.xml) and [catalog/minio.properties](conf/trino/etc/catalog/minio.properties) in order to integrate HMS, MinIO and trino.

The basic autentication scheme in MinIO for buckets is based on S3 access tokens. Exploring other authentication methods is out of scope of this repo. **The keys used in this repo are disposable, created adhoc in a VM**. I decided to hardcode them in order to keep things simple for the reader. 

Configure `s3cmd` by creating file ~/.s3cfg:

```bash
# Setup endpoint
host_base = localhost:9000
host_bucket = localhost:9000
use_https = False

# Setup access keys
access_key = cTI5BM9ecjv6qISgGaHP
secret_key = gJslk7jC1IJqOpDAVoV0fPXFS0WKDcSX9zBGd3f1

# Enable S3 v4 signature APIs
signature_v2 = False
```

To create a bucket called `minio-dlk` and upload data to minio, type:

```bash
s3cmd mb s3://minio-dlk
s3cmd put data/sales_summary.parquet s3://minio-dlk/sales/sales_summary.parquet
```
To list all object in all buckets, type:

```bash
s3cmd la
```

## Access Trino

Download trino cli with:

```bash
wget https://repo1.maven.org/maven2/io/trino/trino-cli/425/trino-cli-425-executable.jar \
  -O trino
chmod +x trino
```

Or use trino client installed in the trino container:
```bash
docker exec -it trino trino
```

## Trino DDLs on minio catalog

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
