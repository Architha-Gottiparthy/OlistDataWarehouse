
def fact_order_items_table(cleaned_data, dimensional_data):
    try:
        # Get dimension tables
        dimension_seller_data = dimensional_data["seller"]
        dimension_products_data = dimensional_data["product"]
        dimension_orders_data = dimensional_data["order"]
        dimension_customer_data = dimensional_data["customer"]

        # Fact source
        df = cleaned_data["olist_order_items_dataset.csv"].copy()

        fact_df = df[
            [
                "order_id",
                "order_item_id",
                "product_id",
                "seller_id",
                "price",
                "freight_value",
                "freight_ratio",
                "item_total_value",
                "shipping_limit_date"
            ]
        ]

        # Join Seller Dimension
        fact_df = fact_df.merge(
            dimension_seller_data[["seller_id", "sur_seller_id"]],
            on="seller_id",
            how="left"
        )

        # Join Product Dimension
        fact_df = fact_df.merge(
            dimension_products_data[["product_id", "sur_product_id"]],
            on="product_id",
            how="left"
        )

        # Join Orders Dimension
        fact_df = fact_df.merge(
            dimension_orders_data[["order_id", "sur_order_id", "customer_id"]],
            on="order_id",
            how="left"
        )

        # Join Customer Dimension
        fact_df = fact_df.merge(
            dimension_customer_data[["customer_id", "sur_customer_id"]],
            on="customer_id",
            how="left"
        )

        # Final fact table columns
        fact_df = fact_df[
            [
                "sur_order_id",
                "sur_product_id",
                "sur_seller_id",
                "sur_customer_id",
                "order_item_id",
                "price",
                "freight_value",
                "freight_ratio",
                "item_total_value",
                "shipping_limit_date"
            ]
        ]

        return fact_df

    except Exception as e:
      raise Exception(f"Error in fact_order_items_table: {e}")