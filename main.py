
import text
import transformed_data
import dimension_pipeline
import FactOrderItemTable
import FactPaymentTable




def warehouse_pipeline():
    try:
        # Step 1: Clean data
        cleaned_data = transformed_data.clean_data_pipeline(
            text.json_path,
            text.quantine_table,
            text.bucket_name
        )

        # Step 2: Build dimensions
        dimension_data = dimension_pipeline.dimensions_pipeline_data(cleaned_data)

        # Step 3: Build fact tables
        fact_order_items = FactOrderItemTable.fact_order_items_table(
            cleaned_data,
            dimension_data
        )
        null_count = fact_order_items["order_item_id"].isnull().sum()
        assert null_count == 0, \
        f"{null_count} null values found in fact_order_items.order_item_id"
        fact_payments = FactPaymentTable.fact_payment_table(
            cleaned_data,
            dimension_data
        )
        null_count = fact_payments["order_id"].isnull().sum()
        assert null_count == 0, \
        f"{null_count} null values found in fact_payments.order_id"

        warehouse_data = {
            "dimensions": dimension_data,
            "fact_order_items": fact_order_items,
            "fact_payments": fact_payments
        }
        return warehouse_data
    except Exception as e:
        raise Exception(f"Error in warehouse pipeline: {e}")

if __name__ == "__main__":
    try:
        warehouse_data = warehouse_pipeline()
        print("Warehouse pipeline executed successfully")
    except Exception as e:
        print(f"Pipeline failed: {e}")