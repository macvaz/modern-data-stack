# Modern data stack

Minimal example to run **Trino** with **MinIO** and **Apache Hive standalone metastore (HMS)** on Docker.

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

Connect to `http://localhost:9000` using the `MINIO_USERNAME` and `MINIO_USER_PASSWORD` provided to docker-compose via `.env file` and create a pair of `access key` and `secret key`. This credentials should be added to [metastore-site.xml](conf/hive-metastore/metastore-site.xml) and [catalog/minio.properties](conf/trino/etc/catalog/minio.properties) in order to integrate HMS, MinIO and trino.

The basic autentication scheme in MinIO for buckets is based on S3 access tokens. Exploring other authentication methods is out of scope of this repo. **The keys used in this repo are disposable, created adhoc in a Proxmox VM**. I decided to hardcode them in order to keep things simple for the reader. 

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

## Access Trino with CLI

Download trino cli with:

```bash
wget https://repo1.maven.org/maven2/io/trino/trino-cli/425/trino-cli-425-executable.jar \
  -O trino
chmod +x trino  # Make it executable
```

Or use trino client installed in the trino container:
```bash
docker exec -it trino trino
```

## Trino DDLs on MinIO

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
