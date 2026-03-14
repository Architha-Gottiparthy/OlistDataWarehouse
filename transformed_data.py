import data_cleaning
import data_transformations
import text

def clean_data_pipeline(json_path,quar_table,bucket_name):
    try:
        data,quarantine_table = data_cleaning.primary_key_null_values(json_path,quar_table,bucket_name)
        cleaned_cat_tables = data_cleaning.null_values_handler(json_path,data)
        cleaned_data = data_cleaning.deduplication_data(json_path,cleaned_cat_tables)
        transformed_data = data_transformations.delivery_status_transformation(cleaned_data,text.status_table_name)
        transformed_data = data_transformations.freight_ratio_transformation(transformed_data,text.table_name)
        transformed_data = data_transformations.item_total_value_transformation(transformed_data,text.table_name)
        transformed_data = data_transformations.product_volume_transformation(transformed_data,text.product_table_name)
        transformed_data = data_transformations.review_status_transformation(transformed_data,text.review_table_name)
        return transformed_data
    except Exception as e:
       raise Exception(f"Error in clean_data_pipeline: {str(e)}")



