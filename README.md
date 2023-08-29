# Modern data stack

Minimal example integrating docker images of the following Big Data open-source projects:

```bash
  - Trino: v425
  - MinIO: v2023.08.23
  - HMS (Hive MetaStore): v3.1.3
```

Since the open-source big data ecosystem is vibrant, this **modern-data-stack is always evolving**. Currently, only the above projects are integrated but in a near future, other complementary and promising projects will be considered like:

```bash
  - OpenMetadata (data catalog)
  - dbt (data transformation)
  - Apache Ranger (data security)
```
## Installation

Install [s3cmd](https://s3tools.org/s3cmd) with:

```bash
sudo apt update
sudo apt install -y \
    s3cmd \
    openjdk-11-jre-headless  # Needed for trino-cli
```

All the components are distributed as **docker containers**, joint together in a [docker-compose file](docker-compose.yml). The compose is mostly self-explanatory. Some additional configuration files for both HMS and trino are also included in this repo.

Start all docker containers with:

```bash
docker-compose up -d
```

Connect to `http://localhost:9000` using the `MINIO_USERNAME` and `MINIO_USER_PASSWORD` provided via `.env file`. **Create a pair of `access key` and `secret key`** in MinIO. This credentials should be added to [metastore-site.xml](docker/hive-metastore/metastore-site.xml) and [catalog/minio.properties](docker/trino/etc/catalog/minio.properties) in order to integrate HMS, MinIO and trino. Since this repo is for teaching purposes, I recommend to create the same keys in order to avoid changing the configuration files.

The basic autentication scheme in MinIO for buckets is based on S3 access tokens. Exploring other authentication methods is out of scope of this repo. **The keys used in this repo are disposable, created adhoc in a one-off VM**. I decided to hardcode them in order to keep things simple for the reader. 

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

## Using Trino

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
