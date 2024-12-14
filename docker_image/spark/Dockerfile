FROM docker.io/bitnami/spark:3.3.2

USER root

# Install prerequisites
# Install prerequisites
RUN apt-get update && apt-get install -y curl && apt-get clean

# Download and move required JAR files
RUN curl -fSL https://repo1.maven.org/maven2/software/amazon/awssdk/s3/2.18.41/s3-2.18.41.jar -o /opt/bitnami/spark/jars/s3-2.18.41.jar \
    && curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.367/aws-java-sdk-1.12.367.jar -o /opt/bitnami/spark/jars/aws-java-sdk-1.12.367.jar \
    && curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.1026.jar \
    && curl -fSL https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar -o /opt/bitnami/spark/jars/delta-core_2.12-2.3.0.jar \
    && curl -fSL https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar -o /opt/bitnami/spark/jars/delta-storage-2.3.0.jar \
    && curl -fSL https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar -o /opt/bitnami/spark/jars/mysql-connector-java-8.0.19.jar \
    && curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.2/hadoop-aws-3.3.2.jar -o /opt/bitnami/spark/jars/hadoop-aws-3.3.2.jar
