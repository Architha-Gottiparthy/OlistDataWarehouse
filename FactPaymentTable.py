
def fact_payment_table(cleaned_data, dimensional_data):
    try:
        # Source table
        data_df = cleaned_data["olist_order_payments_dataset.csv"].copy()

        # Orders dimension
        dim_orders = dimensional_data["order"]

        # Select required columns
        fact_df = data_df[
            ["order_id", "payment_sequential", "payment_installments", "payment_value"]
        ].copy()

        # Join to get surrogate order key
        fact_df = fact_df.merge(
            dim_orders[["order_id", "sur_order_id"]],
            on="order_id",
            how="left"
        )

        # Create surrogate payment key
        fact_df.insert(0, "payment_id", range(1, len(fact_df) + 1))

        # Final fact table columns
        fact_df = fact_df[
            [
                "payment_id",
                "sur_order_id",
                "payment_sequential",
                "payment_installments",
                "payment_value"
            ]
        ]

        return fact_df

    except Exception as e:
       raise Exception(f"Error in fact_payment_table: {e}")