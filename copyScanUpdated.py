import datetime
import typing
import csv
import os
from copy import deepcopy
from functools import cache
import psycopg
import requests
from psycopg.rows import dict_row
from config import *

def fetch_as_dict(cursor) -> typing.Tuple[typing.Dict[str, typing.Any], ...]:
    return tuple(cursor.fetchall())

def run_sql(sql: str, *, instance_name: str, db_password: str) -> typing.Any:
    with psycopg.connect(
            user='proxyuser',
            password=db_password,
            host=f'{instance_name}-maint.rebotics.net',
            port='5432',
            dbname=instance_name,
            row_factory=dict_row
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return fetch_as_dict(cursor)

@cache
def get_auth_token(instance_name: str, username: str, password: str) -> tuple[str, str]:
    response = requests.post(f'https://{instance_name}.rebotics.net/api/v4/token-auth/', json={
        'username': username,
        'password': password,
    })
    response.raise_for_status()
    raw_data = response.json()
    return raw_data['id'], raw_data['token']

SQL_FOR_GETTING_INFO_ABOUT_SCANS = """
SELECT
"realograms_implementation_scan"."id",
"realograms_implementation_scan"."provided_values",
jsonb_build_array((
  SELECT jsonb_build_object('file_id', U0."file_id", 'type', U0."file_type") AS "a"
  FROM "realograms_implementation_scanfile" U0
  WHERE U0."scan_id" = "realograms_implementation_scan"."id"
)) AS "scan_files"
FROM "realograms_implementation_scan"
WHERE "realograms_implementation_scan"."id" IN ({});
"""

def get_info_about_scans(scan_ids: typing.Sequence[int], *, instance_name: str, db_password: str) -> tuple[dict]:
    return run_sql(
        SQL_FOR_GETTING_INFO_ABOUT_SCANS.format(','.join(map(str, scan_ids))),
        instance_name=instance_name,
        db_password=db_password,
    )

def download_file(file_id: int, *, instance_name: str, auth_token: str) -> tuple[str, bytes]:
    response = requests.get(
        f'https://{instance_name}.rebotics.net/api/v1/master-data/file-upload/{file_id}/',
        headers={'Authorization': f'Token {auth_token}'},
    )
    response.raise_for_status()
    raw_data = response.json()
    file_url, file_name = raw_data['file'], raw_data['original_filename']

    response = requests.get(file_url)
    response.raise_for_status()

    return file_name, response.content

def upload_file(*, file_info: tuple, file_type: str, instance_name: str, auth_token: str) -> str:
    response = requests.post(
        f'https://{instance_name}.rebotics.net/api/v4/processing/upload/',
        files={'file': file_info},
        data={'input_type': file_type},
        headers={'Authorization': f'Token {auth_token}'},
    )
    response.raise_for_status()

    return response.json()['id']

def create_scan(data: dict, *, instance_name: str, auth_token: str) -> str:
    response = requests.post(
        f'https://{instance_name}.rebotics.net/api/v4/processing/actions/',
        headers={'Authorization': f'Token {auth_token}'},
        json=data,
    )
    print(data, response.json())
    response.raise_for_status()
    return response.json()['id']

def run(*,
        from_instance: str,
        to_instance: str,
        first_db_password: str,
        second_db_password: str,
        source_username: str,
        source_password: str,
        target_username: str,
        target_password: str,
        scan_ids_for_copying: typing.Sequence[int],
        captured_at:int,
        target_store_id: int,
        output_folder: str = None) -> None:
    try:
        print('Obtaining tokens...')
        _, auth_token_1 = get_auth_token(from_instance, source_username, source_password)
        print(f"Auth token 1 obtained for {from_instance}")
        _, auth_token_2 = get_auth_token(to_instance, target_username, target_password)
        print(f"Auth token 2 obtained for {to_instance}")

        print('Getting info about scans...')
        scans_info = get_info_about_scans(scan_ids_for_copying, instance_name=from_instance, db_password=first_db_password)
        print(f"Retrieved info for {len(scans_info)} scans")

        print('Downloading files...')
        downloaded_files_map = {}
        for scan_info in scans_info:
            for scan_file in scan_info['scan_files']:
                file_id = scan_file['file_id']
                print(f"Downloading file {file_id}...")
                downloaded_files_map[file_id] = download_file(file_id, instance_name=from_instance, auth_token=auth_token_1)
        print(f"Downloaded {len(downloaded_files_map)} files")

        print('Uploading files for scans...')
        uploaded_files_map = {}
        for file_id, file_info in downloaded_files_map.items():
            print(f"Uploading file {file_id}...")
            uploaded_files_map[file_id] = upload_file(
                file_info=file_info,
                instance_name=to_instance,
                auth_token=auth_token_2,
                file_type='image',
            )
        print(f"Uploaded {len(uploaded_files_map)} files")

        print('Creating new scans...')
        new_scan_ids = []
        scan_mapping = []  # List to store source_id -> target_id mapping
        
        for i, scan_info in enumerate(scans_info):
            print(f"Creating scan {i+1}/{len(scans_info)}...")
            source_scan_id = scan_info['id']
            data = deepcopy(scan_info['provided_values']['_raw_data'])
            
            # Remove the specified fields from the data
            fields_to_remove = ['category_id', 'section_id', 'store_planogram', 'aisle']
            for field in fields_to_remove:
                if field in data:
                    del data[field]
                    print(f"Removed field: {field}")
            
            data['store'] = target_store_id
            data['files'] = [uploaded_files_map[scan_file['file_id']] for scan_file in scan_info['scan_files']]
            data['captured_at'] = captured_at

            new_scan_id = create_scan(data=data, instance_name=to_instance, auth_token=auth_token_2)
            new_scan_ids.append(new_scan_id)
            scan_mapping.append((source_scan_id, new_scan_id))

        print('All new scans:', ', '.join(map(str, new_scan_ids)))
        
        # Create CSV file with scan ID mapping
        csv_filename = f"scan_mapping_updated_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        print(f'Creating CSV file: {csv_filename}')
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Source_Scan_ID', 'Target_Scan_ID'])
            writer.writerows(scan_mapping)
        
        print(f'CSV file created successfully: {csv_filename}')
        print('Script completed successfully!')
        
    except Exception as e:
        print(f"Error occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    run(
        from_instance=SOURCE_INSTANCE,
        to_instance=TARGET_INSTANCE,
        source_username=SOURCE_USERNAME,
        source_password=SOURCE_PASSWORD,
        target_username=TARGET_USERNAME,
        target_password=TARGET_PASSWORD,
        first_db_password=SOURCE_DB_PASSWORD,
        second_db_password=TARGET_DB_PASSWORD,
        scan_ids_for_copying=SCAN_IDS_FOR_COPYING,
        captured_at=int(datetime.datetime.now().timestamp()),
        target_store_id=TARGET_STORE_ID,
    )
