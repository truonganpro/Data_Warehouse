from dagster import Definitions, load_assets_from_modules
from dagstermill import ConfigurableLocalOutputNotebookIOManager
from etl_pipeline.assets import bronze, silver, gold, platium
from etl_pipeline.resources.mysql_io_manager import MysqlIOManager
from etl_pipeline.resources.minio_io_manager import MinioIOManager
from etl_pipeline.resources.spark_io_manager import SparkIOManager

bronze_layer_assets = load_assets_from_modules([bronze])
silver_layer_assets = load_assets_from_modules([silver])
gold_layer_assets = load_assets_from_modules([gold])
platium_layer_assets = load_assets_from_modules([platium])

defs = Definitions(
    assets=bronze_layer_assets
    + silver_layer_assets
    + gold_layer_assets
    + platium_layer_assets,
    resources={
        "mysql_io_manager": MysqlIOManager(),
        "minio_io_manager": MinioIOManager(),
        "spark_io_manager": SparkIOManager(),
        "output_notebook_io_manager": ConfigurableLocalOutputNotebookIOManager(),
    },
)