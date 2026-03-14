def dim_customer_scd2(cleaned_data):
    try:
        df_customers = cleaned_data["olist_customers_dataset.csv"].copy()
        df_orders = cleaned_data["olist_orders_dataset.csv"][["customer_id","order_purchase_timestamp"]].copy()

        # Merge customers with order timestamps
        df = df_customers.merge(df_orders, on="customer_id", how="left")

        # Sort by customer and time
        df = df.sort_values(["customer_unique_id","order_purchase_timestamp"]).reset_index(drop=True)

        # Create address key
        df["address"] = (
            df["customer_zip_code_prefix"].astype(str)
            + "_" + df["customer_city"]
            + "_" + df["customer_state"]
        )

        # Detect previous address
        df["prev_address"] = df.groupby("customer_unique_id")["address"].shift()

        # Keep first record OR address change
        df_changes = df[(df["prev_address"].isna()) | (df["address"] != df["prev_address"])].copy()

        # Start date
        df_changes["start_date"] = df_changes["order_purchase_timestamp"]

        # End date (next start date)
        df_changes["end_date"] = df_changes.groupby("customer_unique_id")["start_date"].shift(-1)

        # Current flag
        df_changes["is_current"] = df_changes["end_date"].isna()

        # Surrogate key
        df_changes.insert(0, "sur_customer_id", range(1, len(df_changes) + 1))

        # Select final columns
        final_columns = [
            "sur_customer_id",
            "customer_unique_id",
            "customer_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
            "start_date",
            "end_date",
            "is_current"
        ]

        df_changes = df_changes[final_columns]

        return df_changes

    except Exception as e:
        raise Exception(f"Error in dim_customer_scd2: {str(e)}")