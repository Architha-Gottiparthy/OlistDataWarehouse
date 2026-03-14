import numpy as np

def delivery_status_transformation(cleaned_data,status_table_name):
    try:
        data_df = cleaned_data[status_table_name].copy()
        data_df["delivery_status"] = np.where(
        data_df["order_delivered_customer_date"] > data_df["order_estimated_delivery_date"],
        "late",
        np.where(
          data_df["order_delivered_customer_date"] < data_df["order_estimated_delivery_date"],
          "early",
          "on_time"
        )
        )
        cleaned_data[status_table_name] = data_df
        return cleaned_data
    except Exception as e:
        raise Exception(f"Error occurred while transforming delivery status for table {status_table_name}: {e}")  

def freight_ratio_transformation(cleaned_data,table_name):
  try:
    data_df = cleaned_data[table_name].copy()
    data_df["freight_ratio"] = data_df["freight_value"] / data_df["price"]
    data_df["freight_ratio"] = data_df["freight_ratio"].replace([float("inf")], np.nan)
    cleaned_data[table_name] = data_df
    return cleaned_data
  except Exception as e:
    raise Exception(f"Error occurred while transforming freight ratio for table {table_name}: {e}")
    
def item_total_value_transformation(cleaned_data,table_name):
  try:
    data_df = cleaned_data[table_name]
    data_df["item_total_value"] = (data_df["freight_value"]+data_df["price"])
    cleaned_data[table_name] = data_df
    return cleaned_data
  except Exception as e:
    raise Exception(f"Error occurred while transforming item total value for table {table_name}: {e}")

def product_volume_transformation(cleaned_data,product_table_name):
    try:
        data_df = cleaned_data[product_table_name].copy()
        data_df["product_volume_cm3"] = (data_df["product_length_cm"]*data_df["product_height_cm"]*data_df["product_width_cm"])
        cleaned_data[product_table_name] = data_df
        return cleaned_data
    except Exception as e:
      raise Exception(f"Error occurred while transforming product volume for table {product_table_name}: {e}")



def review_status_transformation(cleaned_data,review_table_name):
  try:
    data_df = cleaned_data[review_table_name].copy()
    data_df["review_status"] = np.where(
      data_df["review_score"] <= 3,
      "Negative",
      "Positive"
      )
    cleaned_data[review_table_name] = data_df
    return cleaned_data
  except Exception as e:
   raise Exception(f"Error occurred while transforming review status for table {review_table_name}: {e}") 