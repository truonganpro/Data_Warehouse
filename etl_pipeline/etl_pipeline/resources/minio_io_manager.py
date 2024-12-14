from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import polars as pl
import pandas as pd
from contextlib import contextmanager
from datetime import datetime
from typing import Union
import os
import pyarrow as pa
import pyarrow.parquet as pq


@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("endpoint_url"),
        access_key=config.get("minio_access_key"),
        secret_key=config.get("minio_secret_key"),
        secure=False,
    )

    try:
        yield client
    except Exception as e:
        raise e


def make_bucket(client: Minio, bucket_name):
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists")


class MinioIOManager(IOManager):
    def __init__(self, config=None):
        self._config = config or {
            "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            "access_key": os.getenv("MINIO_ACCESS_KEY", "minio"),
            "secret_key": os.getenv("MINIO_SECRET_KEY", "minio123"),
            "bucket_name": os.getenv("DATALAKE_BUCKET", "warehouse"),
        }

    def _get_path(self, context: Union[InputContext, OutputContext]):
        # context.asset_key.path: ['bronze', 'schema_name', 'table_name']
        layer, schema, table = context.asset_key.path

        context.log.debug(context.asset_key.path)
        # note: bronze/schema_name/table_name
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        context.log.debug(key)
        # note: /tmp/file_bronze_schema_table_xxxxxxx.parquet
        tmp_file_path = "/tmp/file_{}_{}.parquet".format(
            "_".join(context.asset_key.path), datetime.today().strftime("%Y%m%d%H%M%S")
        )

        if context.has_partition_key:
            # partition_str = table_2020
            partition_str = str(table) + "_" + context.asset_partition_key
            # bronze/schema/table/table_2020.parquet
            # /tmp/file_bronze_schema_table_xxxxxxxxxx.parquet
            return os.path.join(key, f"{partition_str}.parquet"), tmp_file_path
        else:
            # bronze/schema/table.parquet
            return f"{key}.parquet", tmp_file_path



    
    def handle_output(self, context, obj):
        key_name, tmp_file_path = self._get_path(context)

        # Convert Polars DataFrame or Pandas DataFrame to PyArrow Table
        if isinstance(obj, pl.DataFrame):
            table = pa.Table.from_pandas(obj.to_pandas())
        elif isinstance(obj, pd.DataFrame):
            table = pa.Table.from_pandas(obj)
        else:
            raise ValueError("Unsupported DataFrame type for Parquet conversion.")

        # Write to Parquet using PyArrow
        pq.write_table(table, tmp_file_path)

        # Upload to MinIO
        try:
            bucket_name = self._config.get("bucket")
            with connect_minio(self._config) as client:
                make_bucket(client, bucket_name)
                client.fput_object(bucket_name, key_name, tmp_file_path)
                os.remove(tmp_file_path)
        except Exception as e:
            raise e

    def load_input(self, context: InputContext):
        """
        Prepares input and downloads parquet file from MinIO and convert to Pandas DataFrame.
        """
        bucket_name = self._config.get("bucket")
        key_name, tmp_file_path = self._get_path(context)

        try:
            with connect_minio(self._config) as client:
                # Make bucket if not exists
                make_bucket(client, bucket_name)

                context.log.info(f"(MinIO load_input) from key_name: {key_name}")
                client.fget_object(bucket_name, key_name, tmp_file_path)

                # Read as Polars DataFrame
                df_data = pl.read_parquet(tmp_file_path)

                # Convert to Pandas DataFrame
                pandas_df = df_data.to_pandas()

                context.log.info(
                    f"(MinIO load_input) Got Pandas DataFrame with shape: {pandas_df.shape}"
                )

                os.remove(tmp_file_path)
                return pandas_df

        except Exception as e:
            raise e
