from dagster import IOManager, InputContext, OutputContext
from pyspark.sql import SparkSession, DataFrame
import os
from contextlib import contextmanager
from datetime import datetime

@contextmanager
def get_spark_session(config, run_id="Spark IO Manager"):
    try:
        spark = (
            SparkSession.builder.master("spark://spark-master:7077")
            .appName(run_id)
            .config(
                "spark.jars",
                "/usr/local/spark/jars/delta-core_2.12-2.2.0.jar,"
                "/usr/local/spark/jars/hadoop-aws-3.3.2.jar,"
                "/usr/local/spark/jars/aws-java-sdk-1.12.367.jar,"
                "/usr/local/spark/jars/delta-storage-2.2.0.jar"
            )
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.hadoop.fs.s3a.endpoint", f"http://{config['endpoint_url']}")
            .config("spark.hadoop.fs.s3a.access.key", str(config["minio_access_key"]))
            .config("spark.hadoop.fs.s3a.secret.key", str(config["minio_secret_key"]))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.sql.warehouse.dir", "s3a://lakehouse/")
            .getOrCreate()
        )
        yield spark
    except Exception as e:
        raise Exception(f"Error while creating spark session: {e}")

class SparkIOManager(IOManager):
    def __init__(self, config=None):
        # Cấu hình mặc định nếu không truyền vào
        self._config = config or {
            "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "minio_access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
            "minio_secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
        }

    def handle_output(self, context: OutputContext, obj: DataFrame):
        """
        Write output to s3a (aka MinIO) as Delta table
        """
        context.log.debug("(Spark handle_output) Writing output to MinIO ...")

        layer, _, table = context.asset_key.path
        table_name = table.replace(f"{layer}_", "")
        try:
            with get_spark_session(self._config, context.run.run_id) as spark:
                obj.write.format("delta").mode("overwrite").saveAsTable(f"{layer}.{table_name}")
                context.log.debug(f"Saved {table_name} to {layer} layer")
        except Exception as e:
            raise Exception(f"(Spark handle_output) Error while writing output: {e}")

    def load_input(self, context: InputContext) -> DataFrame:
        """
        Load input as Delta table to Spark DataFrame
        """
        context.log.debug(f"Loading input from {context.asset_key.path}...")

        layer, _, table = context.asset_key.path
        table_name = table.replace(f"{layer}_", "")
        try:
            with get_spark_session(self._config) as spark:
                df = spark.read.table(f"{layer}.{table_name}")
                context.log.debug(f"Loaded {df.count()} rows from {table_name}")
                return df
        except Exception as e:
            raise Exception(f"(Spark load_input) Error while loading input: {e}")
