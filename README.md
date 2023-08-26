# Modern data stack

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

Connect to `http://localhost:9000` using the username and password provided to docker-compose in an .env file and create the access key and secret.

Configure `s3cmd` by creating file ~/.s3cfg:

```bash
# Setup endpoint
host_base = localhost:9000
host_bucket = localhost:9000
use_https = False

# Setup access keys
access_key = ACCESS_KEY
secret_key = SECRET_KEY

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
