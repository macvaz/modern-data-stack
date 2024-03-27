#!/bin/bash

SCRIPT_DIR=$(dirname "$(readlink -f $0)")
echo $SCRIPT_DIR

wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -O $SCRIPT_DIR/../jars/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.219/aws-java-sdk-bundle-1.12.219.jar -O $SCRIPT_DIR/../jars/aws-java-sdk-bundle-1.12.219.jar 