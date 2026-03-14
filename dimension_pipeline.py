import dimension_seller
import dimension_product
import dimension_order
import dimension_customer

def dimensions_pipeline_data(data):
    try:
        dim_data = {}
        dim_seller = dimension_seller.dim_seller_table(data)
        dim_data["seller"] = dim_seller
        assert dim_data["seller"]["sur_seller_id"].isnull().sum() == 0, \
            "Null values found in dim_seller surrogate key"
        assert dim_data["seller"]["sur_seller_id"].is_unique, \
            "Duplicate surrogate keys found in dim_seller"
        dim_product = dimension_product.dim_products_table(data)
        dim_data["product"] = dim_product
        assert dim_data["product"]["sur_product_id"].isnull().sum() == 0, \
            "Null values found in dim_product surrogate key"
        assert dim_data["product"]["sur_product_id"].is_unique, \
            "Duplicate surrogate keys found in dim_product"
        dim_order = dimension_order.dim_orders_table(data)
        dim_data["order"] = dim_order
        assert dim_data["order"]["sur_order_id"].isnull().sum() == 0, \
            "Null values found in dim_orders surrogate key"
        assert dim_data["order"]["sur_order_id"].is_unique, \
            "Duplicate surrogate keys found in dim_orders"
        dim_customer = dimension_customer.dim_customer_scd2(data)
        dim_data["customer"] = dim_customer
        assert dim_data["customer"]["sur_customer_id"].isnull().sum() == 0, \
            "Null values found in dim_customer surrogate key"
        assert dim_data["customer"]["sur_customer_id"].is_unique, \
            "Duplicate surrogate keys found in dim_customer"
        return dim_data
    except Exception as e:
        raise Exception(f"Error in dimensions_pipeline_data: {e}")