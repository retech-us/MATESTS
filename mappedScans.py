import datetime
import typing
import csv
import os
import json
from copy import deepcopy
from functools import cache
import psycopg
import requests
from psycopg.rows import dict_row
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import sys
import io

# Fix Windows console encoding to support Unicode characters
if sys.platform == 'win32':
    import codecs
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

def fetch_as_dict(cursor) -> typing.Tuple[typing.Dict[str, typing.Any], ...]:
    return tuple(cursor.fetchall())

def retry_with_exponential_backoff(max_retries=3, base_delay=1, max_delay=60, backoff_factor=2):
    """Decorator to retry functions with exponential backoff for 502/503 errors"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            start_time = time.time()
            max_total_time = 1800  # 30 minutes total timeout for large batches
            
            for attempt in range(max_retries + 1):
                try:
                    # Check if we've exceeded the total timeout
                    if time.time() - start_time > max_total_time:
                        print(f"[ERROR] Total timeout ({max_total_time}s) exceeded", flush=True)
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
        result = download_file(file_id, instance_name=instance_name, auth_token=auth_token)
        return file_id, result
    except Exception as e:
        print(f"[ERROR] [Thread] Error downloading file {file_id}: {e}", flush=True)
        raise

def upload_file_threaded(file_id: int, file_info: tuple, instance_name: str, auth_token: str) -> tuple[int, str]:
    """Thread-safe wrapper for upload_file function"""
    try:
        result = upload_file(
            file_info=file_info,
            instance_name=instance_name,
            auth_token=auth_token,
            file_type='image',
        )
        return file_id, result
    except Exception as e:
        print(f"[ERROR] [Thread] Error uploading file {file_id}: {e}", flush=True)
        raise

def create_scan_threaded(scan_data: dict, source_scan_id: int, instance_name: str, auth_token: str) -> tuple[int, str]:
    """Thread-safe wrapper for create_scan function with better error handling"""
    try:
        print(f"[THREAD] Creating target scan for source scan {source_scan_id}", flush=True)
        result = create_scan(data=scan_data, instance_name=instance_name, auth_token=auth_token)
        print(f"[THREAD] Successfully created target scan {result} for source scan {source_scan_id}", flush=True)
        return source_scan_id, result
    except requests.exceptions.Timeout as e:
        print(f"[ERROR] [Thread] Timeout creating scan for source {source_scan_id}: {e}", flush=True)
        raise
    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] [Thread] HTTP error creating scan for source {source_scan_id}: {e}", flush=True)
        if e.response.status_code == 400:
            print(f"[ERROR] [Thread] 400 Bad Request for source scan {source_scan_id}", flush=True)
            try:
                error_response = e.response.json()
                print(f"[ERROR] [Thread] Error Response: {error_response}", flush=True)
            except:
                print(f"[ERROR] [Thread] Error Response Text: {e.response.text}", flush=True)
            print(f"[ERROR] [Thread] Scan data that caused 400 error: {scan_data}", flush=True)
        else:
            print(f"[ERROR] [Thread] HTTP Status Code: {e.response.status_code}", flush=True)
            try:
                error_response = e.response.json()
                print(f"[ERROR] [Thread] Error Response: {error_response}", flush=True)
            except:
                print(f"[ERROR] [Thread] Error Response Text: {e.response.text}", flush=True)
        raise
    except Exception as e:
        print(f"[ERROR] [Thread] Unexpected error creating scan for source {source_scan_id}: {e}", flush=True)
        print(f"[ERROR] [Thread] Scan data that failed: {scan_data}", flush=True)
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
    
    if raw_data is None:
        print(f"[AUTH] [ERROR] API returned None response for token auth!", flush=True)
        print(f"[AUTH] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v4/token-auth/", flush=True)
        print(f"[AUTH] [ERROR] Username: {username}", flush=True)
        print(f"[AUTH] [ERROR] Response Status: {response.status_code}", flush=True)
        print(f"[AUTH] [ERROR] Response Text: {response.text}", flush=True)
        raise ValueError("API returned None response for token auth")
    
    if 'id' not in raw_data or 'token' not in raw_data:
        print(f"[AUTH] [ERROR] API response missing required fields!", flush=True)
        print(f"[AUTH] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v4/token-auth/", flush=True)
        print(f"[AUTH] [ERROR] Response: {raw_data}", flush=True)
        raise ValueError(f"API response missing required fields. Response: {raw_data}")
    
    return raw_data['id'], raw_data['token']

SQL_FOR_GETTING_INFO_ABOUT_SCANS = """
SELECT
"realograms_implementation_scan"."id",
"realograms_implementation_scan"."provided_values",
jsonb_build_array((
  SELECT jsonb_build_object('file_id', U0."file_id", 'type', U0."file_type") AS "a"
  FROM "realograms_implementation_scanfile" U0
  WHERE U0."scan_id" = "realograms_implementation_scan"."id"
)) AS "scan_files",
"master_data_implementation_category"."name" AS "selected_category_name",
T7."name" AS "pog_category_name"
FROM "realograms_implementation_scan"
LEFT OUTER JOIN "master_data_implementation_category"
ON ("realograms_implementation_scan"."selected_category_id" = "master_data_implementation_category"."id")
LEFT OUTER JOIN "realograms_implementation_realogram"
ON ("realograms_implementation_scan"."active_realogram_id" = "realograms_implementation_realogram"."id")
LEFT OUTER JOIN "planograms_compliance_planogramcompliancereport"
ON ("realograms_implementation_realogram"."id" = "planograms_compliance_planogramcompliancereport"."realogram_id")
LEFT OUTER JOIN "planograms_implementation_planogramstore"
ON ("planograms_compliance_planogramcompliancereport"."store_planogram_id" = "planograms_implementation_planogramstore"."id")
LEFT OUTER JOIN "planograms_implementation_planogram"
ON ("planograms_implementation_planogramstore"."planogram_id" = "planograms_implementation_planogram"."id")
LEFT OUTER JOIN "master_data_implementation_category" T7
ON ("planograms_implementation_planogram"."category_id" = T7."id")
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
        timeout=60  # Increased timeout for large file downloads
    )
    response.raise_for_status()
    raw_data = response.json()
    
    if raw_data is None:
        print(f"[DOWNLOAD] [ERROR] API returned None response for file {file_id}!", flush=True)
        print(f"[DOWNLOAD] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v1/master-data/file-upload/{file_id}/", flush=True)
        print(f"[DOWNLOAD] [ERROR] Response Status: {response.status_code}", flush=True)
        print(f"[DOWNLOAD] [ERROR] Response Text: {response.text}", flush=True)
        raise ValueError(f"API returned None response for file {file_id}")
    
    if 'file' not in raw_data or 'original_filename' not in raw_data:
        print(f"[DOWNLOAD] [ERROR] API response missing required fields for file {file_id}!", flush=True)
        print(f"[DOWNLOAD] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v1/master-data/file-upload/{file_id}/", flush=True)
        print(f"[DOWNLOAD] [ERROR] Response: {raw_data}", flush=True)
        raise ValueError(f"API response missing required fields. Response: {raw_data}")
    
    file_url, file_name = raw_data['file'], raw_data['original_filename']

    response = requests.get(file_url, timeout=120)  # Increased timeout for actual file download
    response.raise_for_status()

    return file_name, response.content

@retry_with_exponential_backoff(max_retries=3, base_delay=2, max_delay=30)
def upload_file(*, file_info: tuple, file_type: str, instance_name: str, auth_token: str) -> str:
    response = requests.post(
        f'https://{instance_name}.rebotics.net/api/v4/processing/upload/',
        files={'file': file_info},
        data={'input_type': file_type},
        headers={'Authorization': f'Token {auth_token}'},
        timeout=120  # Increased timeout for large file uploads
    )
    response.raise_for_status()
    
    response_data = response.json()
    if response_data is None:
        print(f"[UPLOAD] [ERROR] API returned None response!", flush=True)
        print(f"[UPLOAD] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v4/processing/upload/", flush=True)
        print(f"[UPLOAD] [ERROR] File Info: {file_info[0] if file_info else 'None'}", flush=True)
        print(f"[UPLOAD] [ERROR] Response Status: {response.status_code}", flush=True)
        print(f"[UPLOAD] [ERROR] Response Text: {response.text}", flush=True)
        raise ValueError("API returned None response")
    
    if 'id' not in response_data:
        print(f"[UPLOAD] [ERROR] API response missing 'id' field!", flush=True)
        print(f"[UPLOAD] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v4/processing/upload/", flush=True)
        print(f"[UPLOAD] [ERROR] Response: {response_data}", flush=True)
        raise ValueError(f"API response missing 'id' field. Response: {response_data}")
    
    return response_data['id']

@retry_with_exponential_backoff(max_retries=3, base_delay=2, max_delay=30)
def create_scan(data: dict, *, instance_name: str, auth_token: str) -> str:
    # Log the data being sent to create the target scan
    print(f"[CREATE_SCAN] Posting scan data to create target scan:", flush=True)
    print(f"[CREATE_SCAN] Data keys: {list(data.keys())}", flush=True)
    print(f"[CREATE_SCAN] Store: {data.get('store')}, Files: {data.get('files')}, Captured_at: {data.get('captured_at')}", flush=True)
    
    response = requests.post(
        f'https://{instance_name}.rebotics.net/api/v4/processing/actions/',
        headers={'Authorization': f'Token {auth_token}'},
        json=data,
        timeout=60  # Increased timeout for large batches
    )
    
    # Check for 400 Bad Request and log detailed error before raising
    if response.status_code == 400:
        try:
            error_response = response.json()
            print(f"[CREATE_SCAN] [ERROR] 400 Bad Request received:", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Status Code: {response.status_code}", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Response: {error_response}", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Request Data that caused error: {data}", flush=True)
        except:
            print(f"[CREATE_SCAN] [ERROR] 400 Bad Request received:", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Status Code: {response.status_code}", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Response Text: {response.text}", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Request Data that caused error: {data}", flush=True)
    
    response.raise_for_status()
    
    # Safely parse response and check for None
    try:
        response_data = response.json()
        if response_data is None:
            print(f"[CREATE_SCAN] [ERROR] API returned None response!", flush=True)
            print(f"[CREATE_SCAN] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v4/processing/actions/", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Payload sent: {data}", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Response Status: {response.status_code}", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Response Text: {response.text}", flush=True)
            raise ValueError("API returned None response")
        
        if 'id' not in response_data:
            print(f"[CREATE_SCAN] [ERROR] API response missing 'id' field!", flush=True)
            print(f"[CREATE_SCAN] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v4/processing/actions/", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Payload sent: {data}", flush=True)
            print(f"[CREATE_SCAN] [ERROR] Response: {response_data}", flush=True)
            raise ValueError(f"API response missing 'id' field. Response: {response_data}")
        
        target_scan_id = response_data['id']
        
        # Log the response from creating the target scan
        print(f"[CREATE_SCAN] Target scan created successfully:", flush=True)
        print(f"[CREATE_SCAN] Response: {response_data}", flush=True)
        print(f"[CREATE_SCAN] Target Scan ID: {target_scan_id}", flush=True)
        
        return target_scan_id
    except (ValueError, KeyError, TypeError) as e:
        print(f"[CREATE_SCAN] [ERROR] Error parsing API response: {e}", flush=True)
        print(f"[CREATE_SCAN] [ERROR] API URL: https://{instance_name}.rebotics.net/api/v4/processing/actions/", flush=True)
        print(f"[CREATE_SCAN] [ERROR] Payload sent: {data}", flush=True)
        print(f"[CREATE_SCAN] [ERROR] Response Status: {response.status_code}", flush=True)
        try:
            print(f"[CREATE_SCAN] [ERROR] Response JSON: {response.json()}", flush=True)
        except:
            print(f"[CREATE_SCAN] [ERROR] Response Text: {response.text}", flush=True)
        raise

def save_checkpoint(checkpoint_file: str, completed_batches: set, scan_mapping: list, failed_scans: int) -> None:
    """Save progress checkpoint to file"""
    checkpoint_data = {
        'completed_batches': list(completed_batches),
        'scan_mapping': scan_mapping,
        'failed_scans': failed_scans,
        'timestamp': datetime.datetime.now().isoformat()
    }
    try:
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Failed to save checkpoint: {e}", flush=True)

def load_checkpoint(checkpoint_file: str) -> tuple[set, list, int]:
    """Load progress checkpoint from file"""
    if not os.path.exists(checkpoint_file):
        return set(), [], 0
    
    try:
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            checkpoint_data = json.load(f)
        completed_batches = set(checkpoint_data.get('completed_batches', []))
        scan_mapping = checkpoint_data.get('scan_mapping', [])
        failed_scans = checkpoint_data.get('failed_scans', 0)
        print(f"[CHECKPOINT] Loaded checkpoint: {len(completed_batches)} batches completed, {len(scan_mapping)} scans mapped, {failed_scans} failed", flush=True)
        return completed_batches, scan_mapping, failed_scans
    except Exception as e:
        print(f"[WARNING] Failed to load checkpoint: {e}, starting fresh", flush=True)
        return set(), [], 0

def process_batch_with_retry(
    batch_number: int,
    batch_scans: list,
    batch_start: int,
    total_scans: int,
    from_instance: str,
    to_instance: str,
    auth_token_1: str,
    auth_token_2: str,
    target_store_id: int,
    captured_at: int,
    max_batch_retries: int = 3
) -> tuple[list, list, int]:
    """
    Process a single batch with retry logic.
    Returns: (new_scan_ids, scan_mapping, failed_scans)
    """
    batch_count = len(batch_scans)
    
    for retry_attempt in range(max_batch_retries + 1):
        if retry_attempt > 0:
            print(f"[BATCH {batch_number}] Retry attempt {retry_attempt}/{max_batch_retries}", flush=True)
            time.sleep(5 * retry_attempt)  # Exponential backoff between retries
        
        try:
            # ---------------------------
            # Download files for this batch
            # ---------------------------
            downloaded_files_map = {}
            # Safely collect file IDs, handling None scan_files
            file_ids_to_download = []
            for scan_info in batch_scans:
                scan_files = scan_info.get('scan_files', [])
                if scan_files is None:
                    print(f"[BATCH {batch_number}] [WARNING] scan_files is None for scan {scan_info.get('id', 'unknown')}, skipping", flush=True)
                    continue
                for scan_file in scan_files:
                    if scan_file and 'file_id' in scan_file:
                        file_ids_to_download.append(scan_file['file_id'])
            file_ids_to_download = list(set(file_ids_to_download))  # Remove duplicates
            
            if not file_ids_to_download:
                print(f"[BATCH {batch_number}] No files to download (skipping download/upload stage)", flush=True)
            else:
                print(f"[BATCH {batch_number}] Starting download of {len(file_ids_to_download)} files", flush=True)
                max_workers = min(20, len(file_ids_to_download))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    download_futures = {
                        executor.submit(download_file_threaded, file_id, from_instance, auth_token_1): file_id 
                        for file_id in file_ids_to_download
                    }
                    
                    completed_downloads = 0
                    for future in as_completed(download_futures):
                        try:
                            file_id, file_info = future.result()
                            downloaded_files_map[file_id] = file_info
                            completed_downloads += 1
                            if completed_downloads % max(1, min(50, len(file_ids_to_download) // 10)) == 0 or completed_downloads == len(file_ids_to_download):
                                print(f"[BATCH {batch_number}] [DOWNLOAD] {completed_downloads}/{len(file_ids_to_download)} ({completed_downloads*100//len(file_ids_to_download)}%)", flush=True)
                        except Exception as e:
                            file_id = download_futures[future]
                            print(f"[BATCH {batch_number}] [ERROR] Failed to download file {file_id}: {e}", flush=True)
                            if retry_attempt == max_batch_retries:
                                raise  # Re-raise on final attempt
                
                print(f"[BATCH {batch_number}] Downloaded {len(downloaded_files_map)}/{len(file_ids_to_download)} files", flush=True)
                
                # Check if we have enough files downloaded
                if len(downloaded_files_map) < len(file_ids_to_download) * 0.8:  # Less than 80% success
                    if retry_attempt < max_batch_retries:
                        print(f"[BATCH {batch_number}] Too many download failures, retrying batch...", flush=True)
                        continue
                    else:
                        print(f"[BATCH {batch_number}] Too many download failures after {max_batch_retries} retries, continuing with available files", flush=True)
            
            # ---------------------------
            # Upload files for this batch
            # ---------------------------
            uploaded_files_map = {}
            if downloaded_files_map:
                print(f"[BATCH {batch_number}] Starting upload of {len(downloaded_files_map)} files", flush=True)
                max_workers = min(20, len(downloaded_files_map))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    upload_futures = {
                        executor.submit(upload_file_threaded, file_id, file_info, to_instance, auth_token_2): file_id 
                        for file_id, file_info in downloaded_files_map.items()
                    }
                    
                    completed_uploads = 0
                    for future in as_completed(upload_futures):
                        try:
                            file_id, upload_id = future.result()
                            uploaded_files_map[file_id] = upload_id
                            completed_uploads += 1
                            if completed_uploads % max(1, min(50, len(downloaded_files_map) // 10)) == 0 or completed_uploads == len(downloaded_files_map):
                                print(f"[BATCH {batch_number}] [UPLOAD] {completed_uploads}/{len(downloaded_files_map)} ({completed_uploads*100//len(downloaded_files_map)}%)", flush=True)
                        except Exception as e:
                            file_id = upload_futures[future]
                            print(f"[BATCH {batch_number}] [ERROR] Failed to upload file {file_id}: {e}", flush=True)
                            if retry_attempt == max_batch_retries:
                                raise  # Re-raise on final attempt
                
                print(f"[BATCH {batch_number}] Uploaded {len(uploaded_files_map)}/{len(downloaded_files_map)} files", flush=True)
                
                # Check if we have enough files uploaded
                if len(uploaded_files_map) < len(downloaded_files_map) * 0.8:  # Less than 80% success
                    if retry_attempt < max_batch_retries:
                        print(f"[BATCH {batch_number}] Too many upload failures, retrying batch...", flush=True)
                        continue
                    else:
                        print(f"[BATCH {batch_number}] Too many upload failures after {max_batch_retries} retries, continuing with available files", flush=True)
            else:
                print(f"[BATCH {batch_number}] Skipping upload stage (no downloaded files)", flush=True)
            
            # ---------------------------
            # Prepare scan data for this batch
            # ---------------------------
            scan_data_list = []
            for scan_info in batch_scans:
                source_scan_id = scan_info['id']
                # Safely access provided_values and _raw_data
                provided_values = scan_info.get('provided_values')
                print(f"[BATCH {batch_number}] [INFO] Provided values: {provided_values}", flush=True)
                if provided_values is None:
                    print(f"[BATCH {batch_number}] [ERROR] No provided_values for scan {source_scan_id}, skipping", flush=True)
                    continue
                
                # Check if _raw_data exists in provided_values
                if isinstance(provided_values, dict) and '_raw_data' in provided_values:
                    data = deepcopy(provided_values['_raw_data'])
                elif isinstance(provided_values, dict):
                    # If provided_values is a dict but no _raw_data, use provided_values directly
                    data = deepcopy(provided_values)
                else:
                    print(f"[BATCH {batch_number}] [ERROR] Invalid provided_values structure for scan {source_scan_id}, skipping", flush=True)
                    continue
                
                if not isinstance(data, dict):
                    print(f"[BATCH {batch_number}] [ERROR] Invalid data structure for scan {source_scan_id}, skipping", flush=True)
                    continue
                data['store'] = target_store_id
                
                # Safely access scan_files
                scan_files = scan_info.get('scan_files', [])
                if scan_files is None:
                    print(f"[BATCH {batch_number}] [ERROR] scan_files is None for scan {source_scan_id}!", flush=True)
                    print(f"[BATCH {batch_number}] [ERROR] scan_info keys: {list(scan_info.keys())}", flush=True)
                    print(f"[BATCH {batch_number}] [ERROR] scan_info: {scan_info}", flush=True)
                    continue
                
                data['files'] = [uploaded_files_map.get(scan_file.get('file_id')) for scan_file in scan_files if scan_file and 'file_id' in scan_file]
                data['files'] = [file_id for file_id in data['files'] if file_id]  # Filter out missing uploads
                data['captured_at'] = captured_at
                
                # Remove fields that might cause issues in target instance
                fields_to_remove = ['task_id', 'id', 'created_at', 'updated_at']
                for field in fields_to_remove:
                    if field in data:
                        del data[field]
                
                if not data.get('files'):
                    print(f"[BATCH {batch_number}] [WARNING] No files available for source scan {source_scan_id}, skipping scan creation", flush=True)
                    continue
                
                # Log the prepared data for this scan
                print(f"[BATCH {batch_number}] [PREPARE] Prepared scan data for source {source_scan_id}:", flush=True)
                print(f"[BATCH {batch_number}] [PREPARE] Store: {data.get('store')}, Files count: {len(data.get('files', []))}, Captured_at: {data.get('captured_at')}", flush=True)
                
                scan_data_list.append((source_scan_id, data))
            
            if not scan_data_list:
                print(f"[BATCH {batch_number}] No scan data available to create scans", flush=True)
                return [], [], 0
            
            # ---------------------------
            # Create new scans for this batch
            # ---------------------------
            print(f"[BATCH {batch_number}] Creating {len(scan_data_list)} scans", flush=True)
            max_workers = min(15, len(scan_data_list))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                scan_futures = {
                    executor.submit(create_scan_threaded, data, source_scan_id, to_instance, auth_token_2): source_scan_id 
                    for source_scan_id, data in scan_data_list
                }
                
                completed_scans = 0
                failed_scans = 0
                batch_new_scan_ids = []
                batch_scan_mapping = []
                start_time = time.time()
                last_progress_time = start_time
                
                for future in as_completed(scan_futures):
                    try:
                        result = future.result()
                        if result is None:
                            print(f"[BATCH {batch_number}] [ERROR] Future returned None result!", flush=True)
                            source_scan_id = scan_futures[future]
                            print(f"[BATCH {batch_number}] [ERROR] Source scan ID: {source_scan_id}", flush=True)
                            failed_scans += 1
                            continue
                        
                        if not isinstance(result, tuple) or len(result) != 2:
                            print(f"[BATCH {batch_number}] [ERROR] Future returned invalid result format: {result}!", flush=True)
                            source_scan_id = scan_futures[future]
                            print(f"[BATCH {batch_number}] [ERROR] Source scan ID: {source_scan_id}", flush=True)
                            failed_scans += 1
                            continue
                        
                        source_scan_id, new_scan_id = result
                        
                        if new_scan_id is None:
                            print(f"[BATCH {batch_number}] [ERROR] New scan ID is None for source {source_scan_id}!", flush=True)
                            failed_scans += 1
                            continue
                        
                        batch_new_scan_ids.append(new_scan_id)
                        batch_scan_mapping.append((source_scan_id, new_scan_id))
                        completed_scans += 1
                        elapsed_time = time.time() - start_time
                        
                        should_print = (
                            completed_scans % max(1, min(25, len(scan_data_list) // 10)) == 0 or 
                            completed_scans == len(scan_data_list) or
                            (time.time() - last_progress_time) >= 60
                        )
                        
                        if should_print:
                            last_progress_time = time.time()
                            avg_time_per_scan = elapsed_time / completed_scans if completed_scans > 0 else 0
                            remaining_scans = len(scan_data_list) - completed_scans
                            estimated_remaining_time = avg_time_per_scan * remaining_scans if avg_time_per_scan > 0 else 0
                            print(f"[BATCH {batch_number}] [SCAN] {completed_scans}/{len(scan_data_list)} ({completed_scans*100//len(scan_data_list)}%) | Elapsed: {elapsed_time:.1f}s | Est. remaining: {estimated_remaining_time:.1f}s", flush=True)
                            
                    except Exception as e:
                        source_scan_id = scan_futures[future]
                        failed_scans += 1
                        print(f"[BATCH {batch_number}] [ERROR] Failed to create scan for source {source_scan_id}: {e}", flush=True)
                        if failed_scans > len(scan_data_list) * 0.5:
                            print(f"[BATCH {batch_number}] [ERROR] Too many failures ({failed_scans}) in this batch", flush=True)
                            if retry_attempt < max_batch_retries:
                                print(f"[BATCH {batch_number}] Retrying batch...", flush=True)
                                break  # Break out of future processing, will retry
                            else:
                                # Cancel remaining futures on final attempt
                                for f in scan_futures:
                                    if not f.done():
                                        f.cancel()
                                break
            
            # Check if batch was successful enough
            success_rate = completed_scans / len(scan_data_list) if scan_data_list else 0
            if success_rate < 0.5 and retry_attempt < max_batch_retries:
                print(f"[BATCH {batch_number}] Low success rate ({success_rate*100:.1f}%), retrying batch...", flush=True)
                continue
            
            # Batch completed successfully (or on final retry)
            print(f"[BATCH {batch_number}] Completed scan creation for this batch (success: {completed_scans}, failed: {failed_scans})", flush=True)
            return batch_new_scan_ids, batch_scan_mapping, failed_scans
            
        except (TypeError, KeyError, AttributeError) as e:
            # Handle NoneType and subscriptable errors with detailed logging
            error_type = type(e).__name__
            print(f"[BATCH {batch_number}] [ERROR] {error_type} in batch processing: {e}", flush=True)
            print(f"[BATCH {batch_number}] [ERROR] Error details: {str(e)}", flush=True)
            import traceback
            print(f"[BATCH {batch_number}] [ERROR] Traceback:", flush=True)
            traceback.print_exc()
            
            # Log current state
            print(f"[BATCH {batch_number}] [ERROR] Batch state at error:", flush=True)
            print(f"[BATCH {batch_number}] [ERROR] Batch scans count: {len(batch_scans)}", flush=True)
            for idx, scan_info in enumerate(batch_scans):
                print(f"[BATCH {batch_number}] [ERROR] Scan {idx}: ID={scan_info.get('id')}, scan_files={scan_info.get('scan_files')}", flush=True)
            
            if retry_attempt < max_batch_retries:
                print(f"[BATCH {batch_number}] Will retry batch...", flush=True)
                continue
            else:
                print(f"[BATCH {batch_number}] Max retries exceeded, skipping batch", flush=True)
                return [], [], len(batch_scans)  # All scans in batch failed
        except Exception as e:
            print(f"[BATCH {batch_number}] [ERROR] Batch processing failed: {e}", flush=True)
            print(f"[BATCH {batch_number}] [ERROR] Error type: {type(e).__name__}", flush=True)
            import traceback
            print(f"[BATCH {batch_number}] [ERROR] Traceback:", flush=True)
            traceback.print_exc()
            if retry_attempt < max_batch_retries:
                print(f"[BATCH {batch_number}] Will retry batch...", flush=True)
                continue
            else:
                print(f"[BATCH {batch_number}] Max retries exceeded, skipping batch", flush=True)
                return [], [], len(batch_scans)  # All scans in batch failed
    
    # Should never reach here, but just in case
    return [], [], len(batch_scans)

def run(*,
        from_instance: str,
        to_instance: str,
        first_db_password: str,
        second_db_password: str,
        username: str,
        password: str,
        scan_ids_for_copying: typing.Sequence[int],
        captured_at:int,
        target_store_id: int,
        batch_retries: int = 3,
        resume: bool = True) -> None:
    try:
        print('Obtaining tokens...', flush=True)
        _, auth_token_1 = get_auth_token(from_instance, username, password)
        print(f"Auth token 1 obtained for {from_instance}", flush=True)
        _, auth_token_2 = get_auth_token(to_instance, username, password)
        print(f"Auth token 2 obtained for {to_instance}", flush=True)

        print('Getting info about scans...', flush=True)
        scans_info = get_info_about_scans(scan_ids_for_copying, instance_name=from_instance, db_password=first_db_password)
        print(f"Retrieved info for {len(scans_info)} scans", flush=True)

        total_scans = len(scans_info)
        batch_size = 10
        print(f"[BATCH] Processing scans in groups of {batch_size} (total scans: {total_scans})", flush=True)
        print(f"[BATCH] Batch retries enabled: {batch_retries} attempts per batch", flush=True)
        
        # Setup checkpoint file
        checkpoint_file = f"checkpoint_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        if resume:
            print(f"[CHECKPOINT] Checkpoint file: {checkpoint_file}", flush=True)
            # Try to find existing checkpoint file
            checkpoint_files = [f for f in os.listdir('.') if f.startswith('checkpoint_') and f.endswith('.json')]
            if checkpoint_files:
                # Use most recent checkpoint
                checkpoint_file = max(checkpoint_files, key=os.path.getmtime)
                print(f"[CHECKPOINT] Found existing checkpoint: {checkpoint_file}", flush=True)
        
        # Load checkpoint if resuming
        completed_batches = set()
        scan_mapping = []
        total_failed_scans = 0
        if resume:
            completed_batches, scan_mapping, total_failed_scans = load_checkpoint(checkpoint_file)
            if completed_batches:
                print(f"[RESUME] Resuming from batch {max(completed_batches) + 1}, {len(scan_mapping)} scans already completed", flush=True)
        
        new_scan_ids = [mapping[1] for mapping in scan_mapping]  # Extract target scan IDs
        
        # Process batches
        total_batches = (total_scans + batch_size - 1) // batch_size
        for batch_start in range(0, total_scans, batch_size):
            batch_number = (batch_start // batch_size) + 1
            
            # Skip if batch already completed
            if batch_number in completed_batches:
                print(f"\n{'='*80}", flush=True)
                print(f"[BATCH {batch_number}/{total_batches}] Already completed, skipping...", flush=True)
                print(f"{'='*80}\n", flush=True)
                continue
            
            batch_scans = scans_info[batch_start:batch_start + batch_size]
            batch_count = len(batch_scans)
            
            # Extract source scan IDs for this batch
            batch_source_scan_ids = [scan_info['id'] for scan_info in batch_scans]
            
            # Print batch start with scan IDs
            print(f"\n{'='*80}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Starting batch {batch_number}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Processing scans {batch_start + 1}-{batch_start + batch_count} of {total_scans}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Source Scan IDs: {', '.join(map(str, batch_source_scan_ids))}", flush=True)
            print(f"{'='*80}\n", flush=True)
            
            # Process batch with retry logic
            batch_new_scan_ids, batch_scan_mapping, batch_failed = process_batch_with_retry(
                batch_number=batch_number,
                batch_scans=batch_scans,
                batch_start=batch_start,
                total_scans=total_scans,
                from_instance=from_instance,
                to_instance=to_instance,
                auth_token_1=auth_token_1,
                auth_token_2=auth_token_2,
                target_store_id=target_store_id,
                captured_at=captured_at,
                max_batch_retries=batch_retries
            )
            
            # Print batch completion summary
            print(f"\n{'='*80}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Batch {batch_number} completed", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Source Scan IDs: {', '.join(map(str, batch_source_scan_ids))}", flush=True)
            if batch_new_scan_ids:
                print(f"[BATCH {batch_number}/{total_batches}] Created Target Scan IDs: {', '.join(map(str, batch_new_scan_ids))}", flush=True)
            else:
                print(f"[BATCH {batch_number}/{total_batches}] No scans created in this batch", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Success: {len(batch_new_scan_ids)}, Failed: {batch_failed}", flush=True)
            print(f"{'='*80}\n", flush=True)
            
            # Update results
            new_scan_ids.extend(batch_new_scan_ids)
            scan_mapping.extend(batch_scan_mapping)
            total_failed_scans += batch_failed
            
            # Mark batch as completed and save checkpoint
            completed_batches.add(batch_number)
            save_checkpoint(checkpoint_file, completed_batches, scan_mapping, total_failed_scans)
            print(f"[CHECKPOINT] Progress saved: {len(completed_batches)}/{total_batches} batches completed", flush=True)
        
        print(f"[SUCCESS] Created {len(new_scan_ids)} scans successfully across all batches", flush=True)
        total_attempted_scans = total_scans - total_failed_scans
        success_rate = (len(new_scan_ids) / total_attempted_scans * 100) if total_attempted_scans > 0 else 0
        print(f"[STATS] Success rate: {len(new_scan_ids)}/{total_attempted_scans} ({success_rate:.1f}%)", flush=True)
        print(f"[STATS] Failed scans: {total_failed_scans}", flush=True)

        if new_scan_ids:
            print('All new scans:', ', '.join(map(str, new_scan_ids)), flush=True)
        else:
            print("[WARNING] No scans were created successfully!", flush=True)
        
        # Create CSV file with scan ID mapping
        csv_filename = f"scan_mapping_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        print(f'Creating CSV file: {csv_filename}', flush=True)
        
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Source_Scan_ID', 'Target_Scan_ID'])
            writer.writerows(scan_mapping)
        
        print(f'CSV file created successfully: {csv_filename}', flush=True)
        
        # Clean up checkpoint file on successful completion
        if resume and os.path.exists(checkpoint_file):
            try:
                os.remove(checkpoint_file)
                print(f'[CHECKPOINT] Cleaned up checkpoint file: {checkpoint_file}', flush=True)
            except Exception as e:
                print(f'[WARNING] Could not remove checkpoint file: {e}', flush=True)
        
        print('Script completed successfully!', flush=True)
        
    except Exception as e:
        print(f"Error occurred: {e}", flush=True)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    from config import *
    
    run(
        from_instance=SOURCE_INSTANCE,
        to_instance=TARGET_INSTANCE,
        username=SOURCE_USERNAME,
        password=SOURCE_PASSWORD,
        first_db_password=SOURCE_DB_PASSWORD,
        second_db_password=TARGET_DB_PASSWORD,
        scan_ids_for_copying=SCAN_IDS_FOR_COPYING,
        captured_at=int(datetime.datetime.now().timestamp()),
        target_store_id=TARGET_STORE_ID,
    )
