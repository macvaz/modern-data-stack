#!/bin/bash

export JAVA_HOME="$(jrunscript -e 'java.lang.System.out.println(java.lang.System.getProperty("java.home"));')"

# spark
mkdir -p /tmp/spark-events
start-master.sh -p 7077 --webui-port 8061
start-worker.sh spark://spark:7077 --webui-port 8062
start-history-server.sh

# Entrypoint, for example spark-shell, spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$1"
fi