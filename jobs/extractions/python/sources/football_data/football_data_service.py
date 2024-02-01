import requests
import json
import time
from datetime import datetime
from jobs.extractions.python.sources.utils.gs_service import login_to_gs, send_file_to_gs
from jobs.extractions.python.sources.utils.hdfs_service import send_file_to_hdfs, login_to_hdfs

GCS_BUCKET_NAME = 'data-lake-buck'
GCS_JSON_KEY_FILE = '/home/juniortemgoua0/DataLake/utils/credentials/google_storage_secret_key_credentials.json'
HDFS_CLIENT_URL = "http://34.155.106.39:9870"

url = 'https://api.football-data.org/v4/competitions'
matches_url = 'https://api.football-data.org/v4/matches'
team_url = 'https://api.football-data.org/v4/teams'
headers = {'X-Auth-Token': '3338355fcebf4b17b2ccfacbd4eba2b2'}
raw_root_folder = "datalake/raw/football_data"
competitions_root_folder = raw_root_folder + "/competitions"
competitions_standing_root_folder = raw_root_folder + "/standings"
competition_matches_root_folder = raw_root_folder + "/competition_matches"
matches_root_folder = raw_root_folder + "/matches"
players_root_folder = raw_root_folder + "/players"
teams_root_folder = raw_root_folder + "/teams"
current_date_1 = datetime.now()
current_date_as_str_time = current_date_1.strftime('%Y-%m-%d')


def find_competitions(bucket, hdfs_client, current_date: str):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for competition in data['competitions']:
                comp_res = requests.get(url + "/" + competition['code'], headers=headers)
                if comp_res.status_code == 200:
                    comp_res_data = comp_res.json()
                    send_file_to_gs(bucket, json_data=comp_res_data,
                                    bucket_file_path=competitions_root_folder +
                                                     "/" + current_date + "/" + competition[
                                                         'code'] + ".json")
                    send_file_to_hdfs(hdfs_client=hdfs_client, json_data=comp_res_data,
                                      hdfs_path=competitions_root_folder +
                                                "/" + current_date + "/" + competition[
                                                    'code'] + ".json")
                else:
                    print(f"Request failed with code: {comp_res.status_code}")
                time.sleep(8)
                standing_res = requests.get(url + "/" + competition['code'] + "/standings", headers=headers)
                if standing_res.status_code == 200:
                    standing_res_data = standing_res.json()
                    send_file_to_gs(bucket, json_data=standing_res_data,
                                    bucket_file_path=competitions_standing_root_folder +
                                                     "/" + current_date + "/" + competition[
                                                         'code'] + ".json")
                    send_file_to_hdfs(hdfs_client=hdfs_client, json_data=standing_res_data,
                                      hdfs_path=competitions_standing_root_folder +
                                                "/" + current_date + "/" + competition[
                                                    'code'] + ".json")
                else:
                    print(f"Request failed with code: {comp_res.status_code}")
                time.sleep(8)
                matches_res = requests.get(url + "/" + competition['code'] + "/matches", headers=headers)
                if matches_res.status_code == 200:
                    matches_res_data = matches_res.json()
                    send_file_to_gs(bucket, json_data=matches_res_data,
                                    bucket_file_path=competition_matches_root_folder +
                                                     "/" + current_date + "/" + competition[
                                                         'code'] + ".json")
                    send_file_to_hdfs(hdfs_client=hdfs_client, json_data=matches_res_data,
                                      hdfs_path=competition_matches_root_folder +
                                                "/" + current_date + "/" + competition[
                                                    'code'] + ".json")
                else:
                    print(f"Request failed with code: {comp_res.status_code}")
        else:
            print(f"Request failed with code: {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Some error occurred : {e}")


def find_competitions_standing():
    try:
        response = requests.get(url + "/matches", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=2))

        else:
            print(f"Request failed with code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Some error occurred : {e}")


def find_matches(bucket,hdfs_client, current_date: str):
    try:
        response = requests.get(url + "/matches", headers=headers)
        if response.status_code == 200:
            data = response.json()
            send_file_to_gs(bucket, json_data=data,
                            bucket_file_path=matches_root_folder + "/" + current_date + ".json")
            send_file_to_hdfs(hdfs_client=hdfs_client, json_data=data,
                              hdfs_path=matches_root_folder + "/" + current_date + ".json")
        else:
            print(f"Request failed with code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Some error occurred : {e}")


def find_teams(bucket, hdfs_client, current_date: str):
    try:
        response = requests.get(team_url + "?limit=500", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(json.dumps(data, indent=2))
            send_file_to_gs(bucket, json_data=data,
                            bucket_file_path=teams_root_folder + "/" + current_date + ".json")
            send_file_to_hdfs(hdfs_client=hdfs_client, json_data=data,
                              hdfs_path=teams_root_folder + "/" + current_date + ".json")
        else:
            print(f"Request failed with code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Some error occurred : {e}")


def try_request_and_send_data_to_gcs():
    print("Hey")


def extract_data_and_load_to_gs(**kwargs):
    bucket = login_to_gs(bucket_name=GCS_BUCKET_NAME, gcs_json_key_file=GCS_JSON_KEY_FILE)
    hdfs_client = login_to_hdfs(HDFS_CLIENT_URL)
    current_date = kwargs["ds"] if kwargs["ds"] is not None else current_date_as_str_time
    find_competitions(bucket, hdfs_client, current_date)
    find_teams(bucket, hdfs_client, current_date)
