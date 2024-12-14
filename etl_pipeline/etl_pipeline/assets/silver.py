from dagster import asset, AssetIn, Output
import pandas as pd
import polars as pl
import os
# Định nghĩa lại COMPUTE_KIND
COMPUTE_KIND = "Pandas"
LAYER = "silver"

@asset(
    description="Load 'customers' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_customer": AssetIn(
            key_prefix=["bronze", "customer"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "customer"],
    group_name=LAYER,
)
def silver_cleaned_customer(context, bronze_customer: pl.DataFrame):
    """
    Load customers table from bronze layer in MinIO, into a Polars dataframe, then clean data
    """
    # Không cần chuyển đổi sang Pandas, giữ nguyên Polars DataFrame
    context.log.info(f"Loaded bronze data into Polars DataFrame with shape: {bronze_customer.shape}")

    # Làm sạch dữ liệu: Drop duplicates và NaN values
    cleaned_df = bronze_customer.unique().filter(pl.col("*").is_not_null())
    context.log.info(f"Cleaned Polars DataFrame with shape: {cleaned_df.shape}")

    # Return cleaned Polars DataFrame
    return Output(
        value=cleaned_df,
        metadata={
            "row_count": cleaned_df.height,
            "column_count": cleaned_df.width,
            "columns": cleaned_df.columns,
        },
    )


# Silver cleaned seller
@asset(
    description="Load 'seller' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_seller": AssetIn(
            key_prefix=["bronze", "seller"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "seller"],
    group_name=LAYER,
)
def silver_cleaned_seller(context, bronze_seller: pl.DataFrame):
    """
    Load sellers table from bronze layer in MinIO, into a Pandas dataframe, then clean data
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_seller.to_pandas()
    context.log.info(f"Loaded bronze data into Pandas DataFrame with shape: {pandas_df.shape}")

    # Drop duplicates and fill NaN values
    cleaned_df = pandas_df.drop_duplicates(subset=["seller_id"]).fillna("")
    context.log.info(f"Cleaned Pandas DataFrame with shape: {cleaned_df.shape}")

    # Return cleaned Pandas DataFrame
    return Output(
        value=cleaned_df,
        metadata={
            "row_count": cleaned_df.shape[0],
            "column_count": cleaned_df.shape[1],
            "columns": list(cleaned_df.columns),
        },
    )
@asset(
    description="Load 'product' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_product": AssetIn(
            key_prefix=["bronze", "product"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "product"],
    group_name=LAYER,
)
def silver_cleaned_product(context, bronze_product: pl.DataFrame):
    """
    Load product table from bronze layer in MinIO, into a Pandas dataframe, then clean data
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_product.to_pandas()
    context.log.info(f"Loaded bronze data into Pandas DataFrame with shape: {pandas_df.shape}")

    # Drop duplicates and NaN values
    pandas_df = pandas_df.drop_duplicates().dropna()

    # Convert specific columns to integer
    columns_to_convert = [
        "product_description_length",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for column in columns_to_convert:
        if column in pandas_df.columns:
            pandas_df[column] = pandas_df[column].astype(int, errors="ignore")

    context.log.info(f"Cleaned Pandas DataFrame with shape: {pandas_df.shape}")

    # Return cleaned Pandas DataFrame
    return Output(
        value=pandas_df,
        metadata={
            "row_count": pandas_df.shape[0],
            "column_count": pandas_df.shape[1],
            "columns": list(pandas_df.columns),
        },
    )
@asset(
    description="Load 'order_items' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_order_item": AssetIn(
            key_prefix=["bronze", "orderitem"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "orderitem"],
    group_name=LAYER,
)
def silver_cleaned_order_item(context, bronze_order_item: pl.DataFrame):
    """
    Load order_items table from bronze layer in MinIO, into a Pandas dataframe, then clean data
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_order_item.to_pandas()
    context.log.info(f"Loaded bronze data into Pandas DataFrame with shape: {pandas_df.shape}")

    # Drop duplicates and NaN values
    pandas_df = pandas_df.drop_duplicates().dropna()

    # Round numeric columns and ensure proper data types
    if "price" in pandas_df.columns:
        pandas_df["price"] = pandas_df["price"].round(2).astype(float, errors="ignore")
    if "freight_value" in pandas_df.columns:
        pandas_df["freight_value"] = pandas_df["freight_value"].round(2).astype(float, errors="ignore")

    context.log.info(f"Cleaned Pandas DataFrame with shape: {pandas_df.shape}")

    # Return cleaned Pandas DataFrame
    return Output(
        value=pandas_df,
        metadata={
            "row_count": pandas_df.shape[0],
            "column_count": pandas_df.shape[1],
            "columns": list(pandas_df.columns),
        },
    )

@asset(
    description="Load 'payment' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_payment": AssetIn(
            key_prefix=["bronze", "payment"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "payment"],
    group_name=LAYER,
)
def silver_cleaned_payment(context, bronze_payment: pl.DataFrame):
    """
    Load payment table from bronze layer in MinIO, into a Pandas dataframe, then clean data
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_payment.to_pandas()
    context.log.info(f"Loaded bronze data into Pandas DataFrame with shape: {pandas_df.shape}")

    # Drop duplicates and NaN values
    pandas_df = pandas_df.drop_duplicates().dropna()

    # Round and convert numeric columns
    if "payment_value" in pandas_df.columns:
        pandas_df["payment_value"] = pandas_df["payment_value"].round(2).astype(float, errors="ignore")
    if "payment_installments" in pandas_df.columns:
        pandas_df["payment_installments"] = pandas_df["payment_installments"].astype(int, errors="ignore")

    context.log.info(f"Cleaned Pandas DataFrame with shape: {pandas_df.shape}")

    # Return cleaned Pandas DataFrame
    return Output(
        value=pandas_df,
        metadata={
            "row_count": pandas_df.shape[0],
            "column_count": pandas_df.shape[1],
            "columns": list(pandas_df.columns),
        },
    )
@asset(
    description="Load 'order_review' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_order_review": AssetIn(
            key_prefix=["bronze", "orderreview"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "orderreview"],
    group_name="silver",
)
def silver_cleaned_order_review(context, bronze_order_review: pl.DataFrame):
    """
    Load order review table from bronze layer in MinIO, into a Pandas dataframe, then clean data.
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_order_review.to_pandas()
    context.log.info(f"Loaded bronze order review data with shape: {pandas_df.shape}")

    # Drop unnecessary column, duplicates, and null values
    if "review_comment_title" in pandas_df.columns:
        pandas_df = pandas_df.drop(columns=["review_comment_title"])
    pandas_df = pandas_df.drop_duplicates().dropna()

    context.log.info(f"Cleaned order review data, final shape: {pandas_df.shape}")

    return Output(
        value=pandas_df,
        metadata={
            "row_count": pandas_df.shape[0],
            "column_count": pandas_df.shape[1],
            "columns": list(pandas_df.columns),
        },
    )

@asset(
    description="Load 'product_category' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_product_category": AssetIn(
            key_prefix=["bronze", "productcategory"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "productcategory"],
    group_name="silver",
)
def silver_cleaned_product_category(context, bronze_product_category: pl.DataFrame):
    """
    Load product category table from bronze layer in MinIO, into a Pandas dataframe, then clean data.
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_product_category.to_pandas()
    context.log.info(f"Loaded bronze product category data with shape: {pandas_df.shape}")

    # Drop duplicates and null values
    pandas_df = pandas_df.drop_duplicates().dropna()

    context.log.info(f"Cleaned product category data, final shape: {pandas_df.shape}")

    return Output(
        value=pandas_df,
        metadata={
            "row_count": pandas_df.shape[0],
            "column_count": pandas_df.shape[1],
            "columns": list(pandas_df.columns),
        },
    )
@asset(
    description="Load 'order' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_order": AssetIn(
            key_prefix=["bronze", "order"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "order"],
    group_name="silver",
)
def silver_cleaned_order(context, bronze_order: pl.DataFrame):
    """
    Load order table from bronze layer in MinIO, into a Pandas dataframe, then clean data.
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_order.to_pandas()
    context.log.info(f"Loaded bronze order data with shape: {pandas_df.shape}")

    # Drop duplicates and null values, and keep only unique order IDs
    pandas_df = pandas_df.drop_duplicates(subset=["order_id"]).dropna()

    context.log.info(f"Cleaned order data, final shape: {pandas_df.shape}")

    return Output(
        value=pandas_df,
        metadata={
            "row_count": pandas_df.shape[0],
            "column_count": pandas_df.shape[1],
            "columns": list(pandas_df.columns),
        },
    )
@asset(
    description="Generate date dimension table from order data",
    ins={
        "bronze_order": AssetIn(
            key_prefix=["bronze", "order"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind="Pandas",
    key_prefix=["silver", "date"],
    group_name="silver",
)
def silver_date(context, bronze_order):
    """
    Generate date dimension table from order data in MinIO.
    """
    # Kiểm tra kiểu dữ liệu và chuyển đổi nếu cần
    if isinstance(bronze_order, pl.DataFrame):
        pandas_df = bronze_order.to_pandas()
        context.log.info("Converted Polars DataFrame to Pandas DataFrame.")
    elif isinstance(bronze_order, pd.DataFrame):
        pandas_df = bronze_order
        context.log.info("Input is already a Pandas DataFrame.")
    else:
        raise TypeError(
            "Expected bronze_order to be a Pandas or Polars DataFrame, "
            f"but got {type(bronze_order)}."
        )

    context.log.info(f"Loaded bronze order data with shape: {pandas_df.shape}")
    context.log.info(f"Columns in bronze_order: {list(pandas_df.columns)}")

    # Kiểm tra nếu cột 'order_purchase_timestamp' tồn tại
    if "order_purchase_timestamp" not in pandas_df.columns:
        context.log.error("Column 'order_purchase_timestamp' not found in bronze_order.")
        return Output(
            value=pd.DataFrame(),
            metadata={
                "row_count": 0,
                "column_count": 0,
                "columns": [],
            },
        )

    # Chuyển đổi cột timestamp sang datetime
    pandas_df["order_purchase_timestamp"] = pd.to_datetime(
        pandas_df["order_purchase_timestamp"], errors="coerce"
    )
    pandas_df = pandas_df.dropna(subset=["order_purchase_timestamp"])

    # Tạo bảng dimension từ các timestamp duy nhất
    date_df = pandas_df[["order_purchase_timestamp"]].drop_duplicates().reset_index(drop=True)

    # Thêm các cột dimension khác
    date_df["full_date"] = date_df["order_purchase_timestamp"].dt.date
    date_df["year"] = date_df["order_purchase_timestamp"].dt.year
    date_df["month"] = date_df["order_purchase_timestamp"].dt.month
    date_df["day"] = date_df["order_purchase_timestamp"].dt.day
    date_df["weekday"] = date_df["order_purchase_timestamp"].dt.day_name()

    context.log.info(f"Generated date dimension table with shape: {date_df.shape}")

    return Output(
        value=date_df,
        metadata={
            "row_count": date_df.shape[0],
            "column_count": date_df.shape[1],
            "columns": list(date_df.columns),
        },
    )

@asset(
    description="Load 'geo' table from bronze layer in MinIO, into a Pandas dataframe, then clean data",
    ins={
        "bronze_geolocation": AssetIn(
            key_prefix=["bronze", "geolocation"],
        ),
    },
    io_manager_key="minio_io_manager",
    compute_kind=COMPUTE_KIND,
    key_prefix=["silver", "geolocation"],
    group_name="silver",
)
def silver_cleaned_geolocation(context, bronze_geolocation: pl.DataFrame):
    """
    Load geolocation table from bronze layer in MinIO, into a Pandas dataframe, then clean data.
    """
    # Convert Polars DataFrame to Pandas
    pandas_df = bronze_geolocation.to_pandas()
    context.log.info(f"Loaded bronze geolocation data with shape: {pandas_df.shape}")

    # Drop duplicates and NaN values
    pandas_df = pandas_df.drop_duplicates().dropna()

    # Filter coordinates to match the geographical bounds of Brazil
    latitude_col = "geolocation_lat"
    longitude_col = "geolocation_lng"

    if latitude_col in pandas_df.columns and longitude_col in pandas_df.columns:
        pandas_df = pandas_df[
            (pandas_df[latitude_col] <= 5.27438888)
            & (pandas_df[longitude_col] >= -73.98283055)
            & (pandas_df[latitude_col] >= -33.75116944)
            & (pandas_df[longitude_col] <= -34.79314722)
        ]
        context.log.info(f"Filtered geolocation data, final shape: {pandas_df.shape}")
    else:
        context.log.warning(f"Columns {latitude_col} or {longitude_col} not found in data.")

    return Output(
        value=pandas_df,
        metadata={
            "row_count": pandas_df.shape[0],
            "column_count": pandas_df.shape[1],
            "columns": list(pandas_df.columns),
        },
    )
