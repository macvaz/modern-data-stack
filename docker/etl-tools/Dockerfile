  # syntax=docker/dockerfile:1
  FROM python:3.9-bullseye

  ENV SPARK_HOME=/opt/spark
  ENV KYUUBI_HOME=/opt/kyuubi
  ENV BIN_DIR=/usr/bin

  ENV DBT_DIR=/var/lib/dbt
  ENV DATA_STAGE_DIR=/var/lib/stage

  # Add iceberg spark runtime jar to IJava classpath
  ENV IJAVA_CLASSPATH=/opt/spark/jars/*
  ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"
  ENV INSTALL_DIR=/tmp/install

  RUN mkdir -p ${SPARK_HOME} ${KYUUBI_HOME} ${DBT_DIR} ${DATA_STAGE_DIR} ${INSTALL_DIR} 

  # install core packages 
  RUN apt-get update && apt-get upgrade -y && \
      apt-get install -y --no-install-recommends \
        sudo \
        curl \
        unzip \
        make \
        openjdk-11-jdk \
        build-essential \
        software-properties-common \
        libpq-dev \
        gcc \
        g++ \
        libsasl2-dev \
        libsasl2-2\ 
        libsasl2-modules-gssapi-mit\
        unixodbc-dev \
        ssh \
        postgresql-client \
      && rm -rf /var/lib/apt/lists/*

  WORKDIR ${INSTALL_DIR}

  # Install python deps (dbt included)
  COPY requirements.txt .
  RUN pip3 install --no-cache-dir -r requirements.txt && rm requirements.txt \
  # Download spark distribution and install it
    && curl https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o spark-3.3.1-bin-hadoop3.tgz \
      && tar xvzf spark-3.3.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
      && rm spark-3.3.1-bin-hadoop3.tgz \
  # Download iceberg spark runtime
    && curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.1.0/iceberg-spark-runtime-3.3_2.12-1.1.0.jar -Lo iceberg-spark-runtime-3.3_2.12-1.1.0.jar  \
      && mv iceberg-spark-runtime-3.3_2.12-1.1.0.jar /opt/spark/jars \
  # Download Java AWS SDK
    && curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.247/bundle-2.17.247.jar -Lo bundle-2.17.247.jar \
      && mv bundle-2.17.247.jar /opt/spark/jars \
  # Download URL connection client required for S3FileIO
    && curl https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.17.247/url-connection-client-2.17.247.jar -Lo url-connection-client-2.17.247.jar \
      && mv url-connection-client-2.17.247.jar /opt/spark/jars \
  # Install AWS CLI
    && curl https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip -o awscliv2.zip \
      && unzip awscliv2.zip \
      && sudo ./aws/install \
      && rm awscliv2.zip \
      && rm -rf aws/

  WORKDIR ${SPARK_HOME}

  COPY conf/spark-defaults.conf ${SPARK_HOME}/conf
  COPY scripts/entrypoint.sh ${BIN_DIR}

  RUN chmod u+x ${SPARK_HOME}/* && chmod u+x ${SPARK_HOME}/bin/* 

  EXPOSE 3070
  EXPOSE 8888
  EXPOSE 7077
  EXPOSE 8061
  EXPOSE 8062
  EXPOSE 10000
  EXPOSE 10009
  EXPOSE 18080

  ENTRYPOINT ["/usr/bin/entrypoint.sh"]