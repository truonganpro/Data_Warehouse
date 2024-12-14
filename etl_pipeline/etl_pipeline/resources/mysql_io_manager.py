from dagster import IOManager, InputContext, OutputContext
from contextlib import contextmanager
from sqlalchemy import create_engine # type: ignore
import polars as pl
import os
import pandas as pd

def connect_mysql(config) -> str:
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    return conn_info


class MysqlIOManager(IOManager):
    def __init__(self, config=None):
        self._config = config or {
            "host": os.getenv("MYSQL_HOST", "de_mysql"),
            "port": int(os.getenv("MYSQL_PORT", 3306)),
            "user": os.getenv("MYSQL_USER", "admin"),
            "password": os.getenv("MYSQL_PASSWORD", "admin123"),
            "database": os.getenv("MYSQL_DATABASE", "brazillian_ecommerce"),
        }

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        pass

    def load_input(self, context: InputContext):
        pass



    def extract_data(self, sql: str) -> pl.DataFrame:
        conn_info = connect_mysql(self._config)
        # Tạo kết nối SQLAlchemy
        engine = create_engine(conn_info)
        with engine.connect() as connection:
            # Thực thi truy vấn và chuyển đổi kết quả sang Pandas DataFrame
            pandas_df = pd.read_sql(sql, connection)
            # Chuyển đổi Pandas DataFrame thành Polars DataFrame
            df_data = pl.from_pandas(pandas_df)
        return df_data
