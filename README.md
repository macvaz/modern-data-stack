# trino-minio-docker

Minimal example to run Trino with Minio and the Hive standalone metastore on Docker. The data used in this tutorial is from Microsoft AdventureWorks database.  

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

Configure `s3cmd` with (or use the `minio.s3cfg` configuration):

```bash
s3cmd --config minio.s3cfg --configure
```

Use the following configuration for the `s3cmd` configuration when prompted:

```
Access Key: minio_access_key
Secret Key: minio_secret_key
Default Region [US]:
S3 Endpoint [s3.amazonaws.com]: localhost:9000
DNS-style bucket+hostname:port template for accessing a bucket [%(bucket)s.s3.amazonaws.com]: localhost:9000
Encryption password:
Path to GPG program [/usr/bin/gpg]:
Use HTTPS protocol [Yes]: no
```

To create a bucket and upload data to minio, type:

```bash
s3cmd --config minio.s3cfg mb s3://minio-dlk
s3cmd --config minio.s3cfg put data/sales_summary.parquet s3://minio-dlk/sales/sales_summary.parquet
```
To list all object in all buckets, type:

```bash
s3cmd --config minio.s3cfg la
```

## Access Trino with CLI and Prepare Table

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

## Create Trino database infrastructure

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
