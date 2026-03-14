import boto3
import pandas as pd
import io


def extract_all_s3_csv(bucket_name):
  try:
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name)
    dataframes = {}
    for obj in response["Contents"]:
      file_name = obj["Key"]
      if file_name.endswith(".csv"):
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        dataframes[file_name] = df
    return dataframes
  except Exception as e:
    raise Exception(f"Error in extract_all_s3_csv: {e}")
  




