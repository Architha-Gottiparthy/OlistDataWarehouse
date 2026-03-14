def dim_orders_table(cleaned_data):
    try:
        df = cleaned_data["olist_orders_dataset.csv"].copy()

        # Remove duplicates
        df_no_duplicates = df.drop_duplicates(subset=['order_id'], keep='first').reset_index(drop=True)

        # Create surrogate key
        df_no_duplicates.insert(0, "sur_order_id", range(1, len(df_no_duplicates) + 1))

        # Keep only required columns
        columns_to_keep = [
            "sur_order_id",
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "delivery_status"
        ]

        df_no_duplicates = df_no_duplicates[columns_to_keep]

        return df_no_duplicates

    except Exception as e:
        raise Exception(f"Error in dim_orders_table: {str(e)}")