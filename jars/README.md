In order to integrate directly Spark with Minio (S3 compatible data lake), without going though a metastore/catalog it's required to add this packages to spark lib path: hadoop-aws-3.3.4.jar and aws-java-sdk-bundle.jar

Theere is a script in bin folder to automate the jar download.