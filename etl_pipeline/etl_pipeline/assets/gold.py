from dagster import asset, AssetIn, Output
import pandas as pd
import os


COMPUTE_KIND = "Pandas"
LAYER = "gold"


@asset(
    description="Dim customer table from silver_cleaned_customer and silver_cleaned_geolocation",
    ins={
        "silver_cleaned_customer": AssetIn(key_prefix=["silver", "customer"]),
        "silver_cleaned_geolocation": AssetIn(key_prefix=["silver", "geolocation"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "dimcustomer"],
    compute_kind="Pandas",
    group_name="gold",
)
def dim_customer(context, silver_cleaned_customer: pd.DataFrame, silver_cleaned_geolocation: pd.DataFrame):
    """
    Create dim_customer table by joining customer and geolocation data.
    """
    context.log.info(f"Customer DataFrame shape: {silver_cleaned_customer.shape}")
    context.log.info(f"Geolocation DataFrame shape: {silver_cleaned_geolocation.shape}")

    # Join customer with geolocation
    merged_df = silver_cleaned_customer.merge(
        silver_cleaned_geolocation,
        left_on="customer_zip_code_prefix",
        right_on="geolocation_zip_code_prefix",
        how="left",
    )

    # Rename columns
    merged_df.rename(
        columns={
            "geolocation_lat": "customer_lat",
            "geolocation_lng": "customer_lng",
        },
        inplace=True,
    )

    # Drop unnecessary columns
    columns_to_drop = [
        "geolocation_city",
        "geolocation_state",
        "customer_zip_code_prefix",
        "geolocation_zip_code_prefix",
    ]
    existing_columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
    context.log.info(f"Dropping columns: {existing_columns_to_drop}")
    merged_df.drop(columns=existing_columns_to_drop, inplace=True)

    # Deduplicate rows based on customer_id
    merged_df.drop_duplicates(subset=["customer_id"], inplace=True)

    # Select final columns
    final_df = merged_df[
        [
            "customer_id",
            "customer_unique_id",
            "customer_lat",
            "customer_lng",
        ]
    ]

    context.log.info(f"Final DataFrame shape: {final_df.shape}")

    return Output(
        value=final_df,
        metadata={
            "table": "dim_customer",
            "row_count": final_df.shape[0],
            "column_count": final_df.shape[1],
            "columns": list(final_df.columns),
        },
    )


@asset(
    description="Generate seller dimension table from silver_cleaned_seller",
    ins={
        "silver_cleaned_seller": AssetIn(key_prefix=["silver", "seller"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "dimseller"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def dim_seller(context, silver_cleaned_seller: pd.DataFrame):
    """
    Generate seller dimension table.
    """
    context.log.info("Processing seller data")

    # Select required columns
    final_df = silver_cleaned_seller[[
        "seller_id",
        "seller_zip_code_prefix",
    ]].drop_duplicates()

    context.log.info(f"Final seller dimension table shape: {final_df.shape}")

    return Output(
        value=final_df,
        metadata={
            "table": "dim_seller",
            "row_count": final_df.shape[0],
            "column_count": final_df.shape[1],
            "columns": list(final_df.columns),
        },
    )


@asset(
    description="Generate review dimension table from silver_cleaned_order_review",
    ins={
        "silver_cleaned_order_review": AssetIn(key_prefix=["silver", "orderreview"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "dimreview"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def dim_review(context, silver_cleaned_order_review: pd.DataFrame):
    """
    Generate review dimension table.
    """
    context.log.info("Processing review data")

    # Select required columns
    final_df = silver_cleaned_order_review[[
        "review_id",
        "review_score",
    ]].drop_duplicates()

    context.log.info(f"Final review dimension table shape: {final_df.shape}")

    return Output(
        value=final_df,
        metadata={
            "table": "dim_review",
            "row_count": final_df.shape[0],
            "column_count": final_df.shape[1],
            "columns": list(final_df.columns),
        },
    )

@asset(
    description="Generate product dimension table from silver_cleaned_product and silver_cleaned_product_category",
    ins={
        "silver_cleaned_product": AssetIn(key_prefix=["silver", "product"]),
        "silver_cleaned_product_category": AssetIn(key_prefix=["silver", "productcategory"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "dimproduct"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def dim_product(context, silver_cleaned_product: pd.DataFrame, silver_cleaned_product_category: pd.DataFrame):
    """
    Generate product dimension table by joining product and product category data.
    """
    context.log.info("Merging product and product category data")

    # Merge the two DataFrames
    merged_df = pd.merge(
        silver_cleaned_product,
        silver_cleaned_product_category,
        on="product_category_name",
        how="inner"
    )

    # Select the required columns
    final_df = merged_df[[
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]].drop_duplicates()

    context.log.info(f"Final product dimension table shape: {final_df.shape}")

    return Output(
        value=final_df,
        metadata={
            "table": "dim_product",
            "row_count": final_df.shape[0],
            "column_count": final_df.shape[1],
            "columns": list(final_df.columns),
        },
    )


@asset(
    description="Generate order dimension table from silver_cleaned_order and silver_cleaned_payment",
    ins={
        "silver_cleaned_order": AssetIn(key_prefix=["silver", "order"]),
        "silver_cleaned_payment": AssetIn(key_prefix=["silver", "payment"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "dimorder"],
    compute_kind=COMPUTE_KIND,
    group_name=LAYER,
)
def dim_order(context, silver_cleaned_order: pd.DataFrame, silver_cleaned_payment: pd.DataFrame):
    """
    Generate order dimension table by joining order and payment data.
    """
    context.log.info("Merging order and payment data")

    # Merge the two DataFrames
    merged_df = pd.merge(
        silver_cleaned_order,
        silver_cleaned_payment,
        on="order_id",
        how="inner"
    )

    # Select the required columns
    final_df = merged_df[[
        "order_id",
        "order_status",
        "payment_type",
    ]].drop_duplicates()

    context.log.info(f"Final order dimension table shape: {final_df.shape}")

    return Output(
        value=final_df,
        metadata={
            "table": "dim_order",
            "row_count": final_df.shape[0],
            "column_count": final_df.shape[1],
            "columns": list(final_df.columns),
        },
    )


@asset(
    description="Dim date table from silver_date",
    ins={
        "silver_date": AssetIn(
            key_prefix=["silver", "date"],
        ),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["gold", "date"],
    compute_kind="Pandas",
    group_name="gold",
)
def dim_date(context, silver_date: pd.DataFrame):
    """
    Generate date dimension table from silver_date.
    """
    context.log.info(f"Shape of silver_date: {silver_date.shape}")
    context.log.info(f"Columns in silver_date: {list(silver_date.columns)}")

    # Kiểm tra nếu cột 'order_purchase_timestamp' tồn tại
    if "order_purchase_timestamp" not in silver_date.columns:
        context.log.error("Column 'order_purchase_timestamp' not found in silver_date DataFrame.")
        return Output(
            value=pd.DataFrame(),
            metadata={
                "row_count": 0,
                "column_count": 0,
                "columns": [],
            },
        )

    # Chuyển đổi cột timestamp sang định dạng datetime
    silver_date["order_purchase_timestamp"] = pd.to_datetime(
        silver_date["order_purchase_timestamp"], errors="coerce"
    )

    # Loại bỏ các giá trị null
    valid_dates = silver_date.dropna(subset=["order_purchase_timestamp"])
    if valid_dates.empty:
        context.log.error("No valid timestamps found in silver_date DataFrame.")
        return Output(
            value=pd.DataFrame(),
            metadata={
                "row_count": 0,
                "column_count": 0,
                "columns": [],
            },
        )

    # Lấy ngày bắt đầu và kết thúc
    start_date = valid_dates["order_purchase_timestamp"].min().date()
    end_date = valid_dates["order_purchase_timestamp"].max().date()
    context.log.info(f"Date range: {start_date} to {end_date}")

    # Tạo bảng date dimension
    date_range = pd.date_range(start=start_date, end=end_date)
    date_df = pd.DataFrame({"full_date": date_range})

    # Thêm các cột phụ thuộc vào ngày
    date_df["dateKey"] = (
        date_df["full_date"].dt.year * 10000
        + date_df["full_date"].dt.month * 100
        + date_df["full_date"].dt.day
    )
    date_df["year"] = date_df["full_date"].dt.year
    date_df["quarter"] = date_df["full_date"].dt.quarter
    date_df["month"] = date_df["full_date"].dt.month
    date_df["week"] = date_df["full_date"].dt.isocalendar().week
    date_df["day"] = date_df["full_date"].dt.day
    date_df["day_of_year"] = date_df["full_date"].dt.dayofyear
    date_df["day_name_of_week"] = date_df["full_date"].dt.day_name()
    date_df["month_name_of_week"] = date_df["full_date"].dt.month_name()

    context.log.info(f"Generated date dimension table with shape: {date_df.shape}")

    return Output(
        value=date_df,
        metadata={
            "table": "dim_date",
            "row_count": date_df.shape[0],
            "column_count": date_df.shape[1],
            "columns": list(date_df.columns),
        },
    )


@asset(
    description="Fact table to star schema SCD1",
    io_manager_key="minio_io_manager",
    ins={
        "dim_customer": AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_seller": AssetIn(key_prefix=["gold", "dimseller"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "dim_order": AssetIn(key_prefix=["gold", "dimorder"]),
        "silver_cleaned_order_item": AssetIn(key_prefix=["silver", "orderitem"]),
        "silver_cleaned_order": AssetIn(key_prefix=["silver", "order"]),
        "silver_cleaned_payment": AssetIn(key_prefix=["silver", "payment"]),
        "silver_cleaned_order_review": AssetIn(key_prefix=["silver", "orderreview"]),
    },
    key_prefix=["gold", "facttable"],
    compute_kind="Pandas",
    group_name="gold",
)
def fact_table(
    context,
    dim_customer: pd.DataFrame,
    dim_seller: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_order: pd.DataFrame,
    silver_cleaned_order_item: pd.DataFrame,
    silver_cleaned_order: pd.DataFrame,
    silver_cleaned_payment: pd.DataFrame,
    silver_cleaned_order_review: pd.DataFrame,
):
    context.log.info("Starting to merge tables for fact table.")

    # Thực hiện merge step by step
    merged_df = silver_cleaned_order.merge(
        silver_cleaned_order_item, on="order_id", how="inner"
    )
    merged_df = merged_df.merge(dim_order, on="order_id", how="inner")
    merged_df = merged_df.merge(dim_product, on="product_id", how="inner")
    merged_df = merged_df.merge(dim_customer, on="customer_id", how="inner")
    merged_df = merged_df.merge(dim_seller, on="seller_id", how="inner")
    merged_df = merged_df.merge(silver_cleaned_payment, on="order_id", how="inner")
    merged_df = merged_df.merge(silver_cleaned_order_review, on="order_id", how="inner")

    # Chọn các cột cần thiết
    fact_table = merged_df[
        [
            "order_id",
            "order_item_id",
            "customer_id",
            "product_id",
            "review_id",
            "seller_id",
            "price",
            "freight_value",
            "payment_value",
            "payment_installments",
            "payment_sequential",
        ]
    ].drop_duplicates()

    context.log.info(f"Fact table shape: {fact_table.shape}")

    return Output(
        value=fact_table,
        metadata={
            "table": "fact_table",
            "row_count": fact_table.shape[0],
            "column_count": fact_table.shape[1],
            "columns": list(fact_table.columns),
        },
    )