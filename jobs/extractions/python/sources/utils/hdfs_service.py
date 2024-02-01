from hdfs import InsecureClient
import json


def login_to_hdfs(hdfs_url):
    try:
        hdfs_client = InsecureClient(hdfs_url)
        return hdfs_client
    except Exception as e:
        print(f"Some error occurred during hdfs connexion: {e}")
        return False


def send_file_to_hdfs(hdfs_client, hdfs_path, json_data):
    try:
        json_str = json.dumps(json_data)
        with hdfs_client.write("/" + hdfs_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(json_str)
        return True
    except Exception as e:
        print(f"Some error occurred during writing to hdfs: {e}")
        return False
