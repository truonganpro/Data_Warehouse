from dagster import asset, AssetIn, Output
import pandas as pd


@asset(
    description="Data for visualizations",
    ins={
        "dim_order": AssetIn(key_prefix=["gold", "dimorder"]),
        "dim_customer": AssetIn(key_prefix=["gold", "dimcustomer"]),
        "dim_seller": AssetIn(key_prefix=["gold", "dimseller"]),
        "dim_product": AssetIn(key_prefix=["gold", "dimproduct"]),
        "fact_table": AssetIn(key_prefix=["gold", "facttable"]),
    },
    io_manager_key="minio_io_manager",
    key_prefix=["platium", "sale"],
    compute_kind="Pandas",
    group_name="platium",
)
def Cube_sale(
    context,
    dim_order: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_seller: pd.DataFrame,
    dim_product: pd.DataFrame,
    fact_table: pd.DataFrame,
):
    """
    Combine multiple dimensions and fact table to generate data for visualization.
    """

    # Log the shape of input dataframes
    context.log.info(f"dim_order shape: {dim_order.shape}")
    context.log.info(f"dim_customer shape: {dim_customer.shape}")
    context.log.info(f"dim_seller shape: {dim_seller.shape}")
    context.log.info(f"dim_product shape: {dim_product.shape}")
    context.log.info(f"fact_table shape: {fact_table.shape}")

    try:
        # Merge dataframes step by step
        data_mart = fact_table.merge(dim_order, on="order_id", how="inner")
        context.log.info(f"After merging with dim_order: {data_mart.shape}")

        data_mart = data_mart.merge(dim_product, on="product_id", how="inner")
        context.log.info(f"After merging with dim_product: {data_mart.shape}")

        data_mart = data_mart.merge(dim_customer, on="customer_id", how="inner")
        context.log.info(f"After merging with dim_customer: {data_mart.shape}")

        data_mart = data_mart.merge(dim_seller, on="seller_id", how="inner")
        context.log.info(f"After merging with dim_seller: {data_mart.shape}")

        # Drop duplicates
        data_mart = data_mart.drop_duplicates(subset=["order_id", "customer_unique_id"])
        context.log.info(f"After dropping duplicates: {data_mart.shape}")

        # Output metadata
        output_metadata = {
            "table": "Cube_sale",
            "row_count": data_mart.shape[0],
            "column_count": data_mart.shape[1],
            "columns": list(data_mart.columns),
        }

        context.log.info(f"Data Mart generated successfully with shape: {data_mart.shape}")

        return Output(value=data_mart, metadata=output_metadata)

    except Exception as e:
        context.log.error(f"Error during processing: {str(e)}")
        raise e