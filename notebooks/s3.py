#!pip install awswrangler

import awswrangler as wr

wr.config.s3_endpoint_url = 'http://minio:9000'

df = wr.s3.read_csv(path='s3://tests/suba/observations.csv', sep=';')