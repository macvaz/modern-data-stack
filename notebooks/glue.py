
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
df = glueContext.create_dynamic_frame.from_catalog(database='poc', table_name='cars_csv_submission_id_1').toDF()

df.printSchema()
df.select("model", "mpg", "gear").show()

df2 = df.filter(df.model != "Toyota Corolla")
df.count()

#This overwrites ONLY the partitions that have been impacted not all dataset
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

(
    df2.select("model", "mpg", "gear")
    .write.option("path", "s3://testddfv1/spark_bucket2/")
    .mode("overwrite")
    .partitionBy("model")
    .bucketBy(16, "mpg")
    .sortBy("gear")
    .format("parquet")
    .option("compression", "snappy")
    .saveAsTable("poc.spark_partition_bucket")
)

job.commit()

"""
CREATE EXTERNAL TABLE poc.spark_partition_bucket(
  mpg double, 
  gear int)
PARTITIONED BY ( 
  model string)
LOCATION 's3://testddfv1/spark_bucket2'
TBLPROPERTIES (
  'format'='parquet',
  'bucketing_format'='spark',  
  'write.compression'='zstd'
)
"""