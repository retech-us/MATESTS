import datetime
import typing
import csv
import os
from copy import deepcopy
from functools import cache
import psycopg
import requests
from psycopg.rows import dict_row
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from config import *

def fetch_as_dict(cursor) -> typing.Tuple[typing.Dict[str, typing.Any], ...]:
    return tuple(cursor.fetchall())

def retry_with_exponential_backoff(max_retries=3, base_delay=1, max_delay=60, backoff_factor=2):
    """Decorator to retry functions with exponential backoff for 502/503 errors"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            start_time = time.time()
            max_total_time = 300  # 5 minutes total timeout
            
            for attempt in range(max_retries + 1):
                try:
                    # Check if we've exceeded the total timeout
                    if time.time() - start_time > max_total_time:
                        print(f"[ERROR] Total timeout ({max_total_time}s) exceeded")
                        raise TimeoutError("Total operation timeout exceeded")
                    
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code in [502, 503]:
                        last_exception = e
                        if attempt < max_retries:
                            # Calculate delay with exponential backoff and jitter
                            delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                            jitter = random.uniform(0, 0.1) * delay
                            total_delay = delay + jitter
                            
                            print(f"[WARNING] HTTP {e.response.status_code} error on attempt {attempt + 1}/{max_retries + 1}")
                            print(f"[RETRY] Retrying in {total_delay:.2f} seconds...")
                            time.sleep(total_delay)
                            continue
                        else:
                            print(f"[ERROR] Max retries ({max_retries}) exceeded for HTTP {e.response.status_code}")
                            raise e
                    else:
                        # Re-raise non-502/503 HTTP errors immediately
                        raise e
                except requests.exceptions.Timeout as e:
                    print(f"[ERROR] Request timeout on attempt {attempt + 1}/{max_retries + 1}")
                    if attempt < max_retries:
                        delay = min(base_delay * (backoff_factor ** attempt), max_delay)
                        print(f"[RETRY] Retrying in {delay:.2f} seconds...")
                        time.sleep(delay)
                        continue
                    else:
                        raise e
                except Exception as e:
                    # Re-raise non-HTTP errors immediately
                    raise e
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator

def download_file_threaded(file_id: int, instance_name: str, auth_token: str) -> tuple[int, tuple]:
    """Thread-safe wrapper for download_file function"""
    try:
        print(f"[THREAD] Downloading file {file_id}...")
        result = download_file(file_id, instance_name=instance_name, auth_token=auth_token)
        print(f"[THREAD] Downloaded file {file_id}")
        return file_id, result
    except Exception as e:
        print(f"[ERROR] [Thread] Error downloading file {file_id}: {e}")
        raise

def upload_file_threaded(file_id: int, file_info: tuple, instance_name: str, auth_token: str) -> tuple[int, str]:
    """Thread-safe wrapper for upload_file function"""
    try:
        print(f"[THREAD] Uploading file {file_id}...")
        result = upload_file(
            file_info=file_info,
            instance_name=instance_name,
            auth_token=auth_token,
            file_type='image',
        )
        print(f"[THREAD] Uploaded file {file_id}")
        return file_id, result
    except Exception as e:
        print(f"[ERROR] [Thread] Error uploading file {file_id}: {e}")
        raise

def create_scan_threaded(scan_data: dict, source_scan_id: int, instance_name: str, auth_token: str) -> tuple[int, str]:
    """Thread-safe wrapper for create_scan function with better error handling"""
    try:
        print(f"[THREAD] Creating scan for source {source_scan_id}...")
        # Log the data being sent for debugging (only keys to avoid spam)
        print(f"[THREAD] Scan data keys: {list(scan_data.keys())}")
        
        # Add a small delay between requests to avoid overwhelming the API
        time.sleep(0.5)
        
        result = create_scan(data=scan_data, instance_name=instance_name, auth_token=auth_token)
        print(f"[THREAD] ✅ Created scan {result} for source {source_scan_id}")
        return source_scan_id, result
    except requests.exceptions.Timeout as e:
        print(f"[ERROR] [Thread] Timeout creating scan for source {source_scan_id}: {e}")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] [Thread] HTTP error creating scan for source {source_scan_id}: {e}")
        if e.response.status_code == 400:
            print(f"[ERROR] [Thread] Bad request - scan data: {scan_data}")
        raise
    except Exception as e:
        print(f"[ERROR] [Thread] Unexpected error creating scan for source {source_scan_id}: {e}")
        print(f"[ERROR] [Thread] Scan data that failed: {scan_data}")
        raise

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
    }, timeout=30)  # Add 30-second timeout
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

@retry_with_exponential_backoff(max_retries=3, base_delay=2, max_delay=30)
def upload_file(*, file_info: tuple, file_type: str, instance_name: str, auth_token: str) -> str:
    response = requests.post(
        f'https://{instance_name}.rebotics.net/api/v4/processing/upload/',
        files={'file': file_info},
        data={'input_type': file_type},
        headers={'Authorization': f'Token {auth_token}'},
    )
    response.raise_for_status()

    return response.json()['id']

@retry_with_exponential_backoff(max_retries=3, base_delay=2, max_delay=30)
def create_scan(data: dict, *, instance_name: str, auth_token: str) -> str:
    response = requests.post(
        f'https://{instance_name}.rebotics.net/api/v4/processing/actions/',
        headers={'Authorization': f'Token {auth_token}'},
        json=data,
        timeout=30  # Add 30-second timeout
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

        print('Downloading files with threading...')
        downloaded_files_map = {}
        
        # Collect all file IDs to download
        file_ids_to_download = []
        for scan_info in scans_info:
            for scan_file in scan_info['scan_files']:
                file_ids_to_download.append(scan_file['file_id'])
        
        print(f"[DOWNLOAD] Starting download of {len(file_ids_to_download)} files using thread pool...")
        
        # Use ThreadPoolExecutor for concurrent downloads
        max_workers = min(5, len(file_ids_to_download))  # Limit to 5 concurrent downloads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all download tasks
            download_futures = {
                executor.submit(download_file_threaded, file_id, from_instance, auth_token_1): file_id 
                for file_id in file_ids_to_download
            }
            
            # Process completed downloads
            completed_downloads = 0
            for future in as_completed(download_futures):
                try:
                    file_id, file_info = future.result()
                    downloaded_files_map[file_id] = file_info
                    completed_downloads += 1
                    print(f"[DOWNLOAD] Progress: {completed_downloads}/{len(file_ids_to_download)} files downloaded")
                except Exception as e:
                    file_id = download_futures[future]
                    print(f"[ERROR] Failed to download file {file_id}: {e}")
        
        print(f"[SUCCESS] Downloaded {len(downloaded_files_map)} files successfully")

        print('Uploading files with threading...')
        uploaded_files_map = {}
        
        print(f"[UPLOAD] Starting upload of {len(downloaded_files_map)} files using thread pool...")
        
        # Use ThreadPoolExecutor for concurrent uploads
        max_workers = min(5, len(downloaded_files_map))  # Limit to 5 concurrent uploads
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all upload tasks
            upload_futures = {
                executor.submit(upload_file_threaded, file_id, file_info, to_instance, auth_token_2): file_id 
                for file_id, file_info in downloaded_files_map.items()
            }
            
            # Process completed uploads
            completed_uploads = 0
            for future in as_completed(upload_futures):
                try:
                    file_id, upload_id = future.result()
                    uploaded_files_map[file_id] = upload_id
                    completed_uploads += 1
                    print(f"[UPLOAD] Progress: {completed_uploads}/{len(downloaded_files_map)} files uploaded")
                except Exception as e:
                    file_id = upload_futures[future]
                    print(f"[ERROR] Failed to upload file {file_id}: {e}")
        
        print(f"[SUCCESS] Uploaded {len(uploaded_files_map)} files successfully")

        print('Creating new scans with threading...')
        new_scan_ids = []
        scan_mapping = []  # List to store source_id -> target_id mapping
        
        # Prepare scan data for all scans
        scan_data_list = []
        for scan_info in scans_info:
            source_scan_id = scan_info['id']
            data = deepcopy(scan_info['provided_values']['_raw_data'])
            
            # Remove the specified fields from the data (including task_id to prevent 400 errors)
            fields_to_remove = ['category_id', 'section_id', 'store_planogram', 'aisle', 'task_id']
            for field in fields_to_remove:
                if field in data:
                    del data[field]
                    print(f"Removed field: {field}")
            
            data['store'] = target_store_id
            data['files'] = [uploaded_files_map[scan_file['file_id']] for scan_file in scan_info['scan_files']]
            data['captured_at'] = captured_at
            
            # Remove additional fields that might cause issues in target instance
            additional_fields_to_remove = ['id', 'created_at', 'updated_at']
            removed_additional_fields = []
            for field in additional_fields_to_remove:
                if field in data:
                    removed_additional_fields.append(field)
                    del data[field]
            
            if removed_additional_fields:
                print(f"Removed additional fields from scan {source_scan_id}: {', '.join(removed_additional_fields)}")
            
            scan_data_list.append((source_scan_id, data))
        
        print(f"[SCAN] Starting creation of {len(scan_data_list)} scans using thread pool...")
        
        # Use ThreadPoolExecutor for concurrent scan creation with timeout handling
        max_workers = min(2, len(scan_data_list))  # Reduce to 2 concurrent scan creations to avoid overwhelming the API
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all scan creation tasks
            scan_futures = {
                executor.submit(create_scan_threaded, data, source_scan_id, to_instance, auth_token_2): source_scan_id 
                for source_scan_id, data in scan_data_list
            }
            
            # Process completed scan creations with timeout handling
            completed_scans = 0
            failed_scans = 0
            start_time = time.time()
            
            for future in as_completed(scan_futures, timeout=300):  # 5-minute total timeout
                try:
                    source_scan_id, new_scan_id = future.result(timeout=60)  # 1-minute timeout per scan
                    new_scan_ids.append(new_scan_id)
                    scan_mapping.append((source_scan_id, new_scan_id))
                    completed_scans += 1
                    elapsed_time = time.time() - start_time
                    print(f"[SCAN] Progress: {completed_scans}/{len(scan_data_list)} scans created (Elapsed: {elapsed_time:.1f}s)")
                except Exception as e:
                    source_scan_id = scan_futures[future]
                    failed_scans += 1
                    print(f"[ERROR] Failed to create scan for source {source_scan_id}: {e}")
                    if failed_scans > len(scan_data_list) * 0.5:  # If more than 50% fail, stop
                        print(f"[ERROR] Too many failures ({failed_scans}), stopping execution")
                        break
        
        print(f"[SUCCESS] Created {len(new_scan_ids)} scans successfully")
        print(f"[STATS] Success rate: {len(new_scan_ids)}/{len(scan_data_list)} ({len(new_scan_ids)/len(scan_data_list)*100:.1f}%)")
        print(f"[STATS] Failed scans: {failed_scans}")

        if new_scan_ids:
            print('All new scans:', ', '.join(map(str, new_scan_ids)))
        else:
            print("[WARNING] No scans were created successfully!")
        
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
