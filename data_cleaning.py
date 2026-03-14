
import numpy as np
import meta_data
import data_extract
import pandas as pd

def primary_key_null_values(path, quarantine_table, BUCKET_NAME):
    try:
        metaData = meta_data.metadata_table(path)
        table_name_list = list(metaData.keys())

        data = data_extract.extract_all_s3_csv(BUCKET_NAME)

        for table_name in table_name_list:
            table_name_path = table_name + ".csv"

            not_null_columns_list = metaData[table_name]["not_null_columns"]

            df = data[table_name_path]

            # rows where any not-null column has null
            mask = df[not_null_columns_list].isnull().any(axis=1)

            null_rows = df[mask]

            if not null_rows.empty:
                quarantine_table = pd.concat([quarantine_table, null_rows], ignore_index=True)

                # drop them from original dataset
                data[table_name_path] = df[~mask]

            else:
                print(f"{table_name} → No Null Values")

        return data, quarantine_table

    except Exception as e:
        raise Exception(f"Error in primary_key_null_values: {e}")




def null_values_handler(path,data):
    try:
        # metadata
        metaData = meta_data.metadata_table(path)

        # extract all tables once

        for table_name, column_types in metaData.items():

            table_name_path = table_name + ".csv"
            df = data[table_name_path]

            numeric_cols = column_types["numeric"]
            object_cols = column_types["no_data"]
            date_cols = column_types["date"]

            # -------- numeric columns --------
            for col in numeric_cols:
                if df[col].isna().sum() > 0:
                    print(f"Filling numeric nulls in {table_name}.{col}")
                    df[col] = df[col].fillna(0)

            # -------- object columns --------
            for col in object_cols:
                if df[col].isna().sum() > 0:
                    print(f"Filling object nulls in {table_name}.{col}")
                    df[col] = df[col].fillna("No Data")

            # -------- date columns --------
            for col in date_cols:
                if df[col].isna().sum() > 0:
                    print(f"Filling date nulls in {table_name}.{col}")
                    df[col] = df[col].fillna(pd.Timestamp("1900-01-01"))

            # save updated dataframe back
            data[table_name_path] = df

        return data

    except Exception as e:
        raise Exception(f"Error in null_values_handler: {e}")

def deduplication_data(path,data):
  try:
    cleaned_data = {}
    metaData = meta_data.metadata_table(path)
    for table_name, column_types in metaData.items():
        table_name_path = table_name + ".csv"
        df = data[table_name_path]
        if df.duplicated().sum() > 0:
            df = df.drop_duplicates(keep='first')
        else:
            print("No duplicates")
            
        cleaned_data[table_name_path] = df
    return cleaned_data
  except Exception as e:
    raise Exception(f"Error in deduplication_data: {e}")

