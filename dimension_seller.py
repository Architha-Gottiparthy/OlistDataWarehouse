def dim_seller_table(data):
    try:
        df = data["olist_sellers_dataset.csv"].copy()

        # Remove duplicates based on seller_id
        df_no_duplicates = df.drop_duplicates(subset=['seller_id'], keep='first').reset_index(drop=True)

        # Create surrogate key
        df_no_duplicates.insert(0, "sur_seller_id", range(1, len(df_no_duplicates) + 1))

        return df_no_duplicates

    except Exception as e:
        raise Exception(f"Error in dim_seller_table: {str(e)}")
    
