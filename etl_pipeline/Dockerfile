FROM python:3.9.16-slim

# Install Spark and Java
ARG openjdk_version="17"
RUN apt-get update --yes && \
    apt-get install --yes curl "openjdk-${openjdk_version}-jre-headless" \
    ca-certificates-java procps build-essential gcc g++ cmake libffi-dev libssl-dev libxml2-dev libxslt1-dev zlib1g-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Download Spark necessary jars
RUN curl -O https://archive.apache.org/dist/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz \
  && tar zxvf spark-3.3.2-bin-hadoop3.tgz \
  && rm -rf spark-3.3.2-bin-hadoop3.tgz \
  && mv spark-3.3.2-bin-hadoop3/ /usr/local/ \
  && ln -s /usr/local/spark-3.3.2-bin-hadoop3 /usr/local/spark

# Download AWS and Hadoop JARs
RUN curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar \
  && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar \
  && curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
  && curl -O https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.2.0/delta-core_2.12-2.2.0.jar \
  && curl -O https://repo1.maven.org/maven2/io/delta/delta-storage/2.2.0/delta-storage-2.2.0.jar \
  && curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar \
  && mv s3-2.18.41.jar /usr/local/spark/jars \
  && mv aws-java-sdk-1.12.367.jar /usr/local/spark/jars \
  && mv aws-java-sdk-bundle-1.11.1026.jar /usr/local/spark/jars \
  && mv delta-core_2.12-2.2.0.jar /usr/local/spark/jars \
  && mv delta-storage-2.2.0.jar /usr/local/spark/jars \
  && mv hadoop-aws-3.3.2.jar /usr/local/spark/jars

# Set working directory
WORKDIR /opt/dagster/app/

# Copy requirements and install dependencies
COPY requirements.txt /opt/dagster/app/etl_pipeline/
RUN pip install --upgrade pip && pip install --no-cache-dir -r etl_pipeline/requirements.txt

# Copy application code
COPY etl_pipeline /opt/dagster/app/etl_pipeline

# Ensure `etl_pipeline` is discoverable as a Python module
ENV PYTHONPATH="/opt/dagster/app:${PYTHONPATH}"
ENV SPARK_CLASSPATH="/usr/local/spark/jars/*"

# Default command to run Dagster API server
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]