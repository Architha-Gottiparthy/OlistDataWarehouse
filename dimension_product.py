def dim_products_table(cleaned_data):
    try:
        df = cleaned_data["olist_products_dataset.csv"].copy()

        # Remove duplicates based on product_id
        df_no_duplicates = df.drop_duplicates(subset=['product_id'], keep='first').reset_index(drop=True)

        # Create surrogate key
        df_no_duplicates.insert(0, "sur_product_id", range(1, len(df_no_duplicates) + 1))

        return df_no_duplicates

    except Exception as e:
       raise Exception(f"Error in dim_products_table: {str(e)}")

