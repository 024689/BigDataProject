import requests
import json
import time
from datetime import datetime
from jobs.extractions.python.sources.utils.gs_service import login_to_gs, send_file_to_gs

GCS_BUCKET_NAME = 'data-lake-buck'
GCS_JSON_KEY_FILE = ('../../../../../../../utils/credentials'
                     '/google_storage_secret_key_credentials.json')

url = 'https://api.football-data.org/v4/competitions'
matches_url = 'https://api.football-data.org/v4/matches'
headers = {'X-Auth-Token': '3338355fcebf4b17b2ccfacbd4eba2b2'}
raw_root_folder = "raw/football_data"
competitions_root_folder = raw_root_folder + "/competitions"
competitions_standing_root_folder = raw_root_folder + "/standings"
competition_matches_root_folder = raw_root_folder + "/competition_matches"
matches_root_folder = raw_root_folder + "/matches"
players_root_folder = raw_root_folder + "/players"
current_date = date_du_jour = datetime.now()
current_date_as_str_time = current_date.strftime('%Y-%m-%d')


def find_competitions():
    try:
        bucket = login_to_gs(bucket_name=GCS_BUCKET_NAME, gcs_json_key_file=GCS_JSON_KEY_FILE)
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(data)
            for competition in data['competitions']:
                print(competition['code'])
                comp_res = requests.get(url + "/" + competition['code'], headers=headers)
                if comp_res.status_code == 200:
                    comp_res_data = comp_res.json()
                    send_file_to_gs(bucket, json_data=comp_res_data,
                                    bucket_file_path=competitions_root_folder +
                                                     "/" + current_date_as_str_time + "/" + competition[
                                                         'code'] + ".json")
                else:
                    print(f"Request failed with code: {comp_res.status_code}")
                time.sleep(6)
                standing_res = requests.get(url + "/" + competition['code'] + "/standings", headers=headers)
                if standing_res.status_code == 200:
                    standing_res_data = standing_res.json()
                    send_file_to_gs(bucket, json_data=standing_res_data,
                                    bucket_file_path=competitions_standing_root_folder +
                                                     "/" + current_date_as_str_time + "/" + competition[
                                                         'code'] + ".json")
                else:
                    print(f"Request failed with code: {comp_res.status_code}")
                time.sleep(6)
                matches_res = requests.get(url + "/" + competition['code'] + "/matches", headers=headers)
                if matches_res.status_code == 200:
                    matches_res_data = matches_res.json()
                    send_file_to_gs(bucket, json_data=matches_res_data,
                                    bucket_file_path=competition_matches_root_folder +
                                                     "/" + current_date_as_str_time + "/" + competition[
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


def find_matches():
    try:
        bucket = login_to_gs(bucket_name=GCS_BUCKET_NAME, gcs_json_key_file=GCS_JSON_KEY_FILE)
        response = requests.get(url + "/matches", headers=headers)
        if response.status_code == 200:
            data = response.json()
            send_file_to_gs(bucket, json_data=data,
                            bucket_file_path=matches_root_folder
                                             + "/" + current_date_as_str_time + ".json")
        else:
            print(f"Request failed with code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Some error occurred : {e}")


def find_matches2():
    try:
        # bucket = login_to_gs(bucket_name=GCS_BUCKET_NAME, gcs_json_key_file=GCS_JSON_KEY_FILE)
        response = requests.get(matches_url + "/330299", headers=headers)
        if response.status_code == 200:
            data = response.json()
            print(data)
            # send_file_to_gs(bucket, json_data=data,
            #                 bucket_file_path=matches_root_folder
            #                                  + "/" + current_date_as_str_time + ".json")
        else:
            print(f"Request failed with code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Some error occurred : {e}")
    return


def try_request_and_send_data_to_gcs():
    print("Hey")


def extract_data_and_load_to_gs():
    # find_competitions()
    find_matches2()


extract_data_and_load_to_gs()
