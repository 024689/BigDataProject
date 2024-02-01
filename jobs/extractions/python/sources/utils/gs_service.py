import json

from google.cloud import storage


def login_to_gs(bucket_name: str, gcs_json_key_file: str):
    try:
        client = storage.Client.from_service_account_json(gcs_json_key_file)
        bucket = client.get_bucket(bucket_name)
        print(f"Login to bucket {bucket_name}")
        return bucket
    except Exception as e:
        print(f"Some error occurred during cloud Storage connexion: {e}")
        return None


def send_file_to_gs(bucket, json_data, bucket_file_path):
    try:
        blob = bucket.blob(bucket_file_path)
        json_data_stringify = json.dumps(json_data, indent=2)
        blob.upload_from_string(json_data_stringify)
        print(f"File was send successfully as {bucket_file_path}")
    except Exception as e:
        print(f"Some error occurred during writing for gcs: {e}")
