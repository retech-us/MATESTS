import datetime
import typing
import os
import sys
import io
import argparse
from functools import cache
import psycopg
import requests
from psycopg.rows import dict_row
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import *

# Fix Windows console encoding to support Unicode characters
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

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
            return tuple(cursor.fetchall())

@cache
def get_auth_token(instance_name: str, username: str, password: str) -> tuple[str, str]:
    response = requests.post(f'https://{instance_name}.rebotics.net/api/v4/token-auth/', json={
        'username': username,
        'password': password,
    }, timeout=30)
    response.raise_for_status()
    raw_data = response.json()
    
    if raw_data is None:
        print(f"[AUTH] [ERROR] API returned None response for token auth!", flush=True)
        raise ValueError("API returned None response for token auth")
    
    if 'id' not in raw_data or 'token' not in raw_data:
        print(f"[AUTH] [ERROR] API response missing required fields!", flush=True)
        raise ValueError(f"API response missing required fields. Response: {raw_data}")
    
    return raw_data['id'], raw_data['token']

SQL_FOR_GETTING_INFO_ABOUT_SCANS = """
SELECT DISTINCT ON ("realograms_implementation_scan"."id")
"realograms_implementation_scan"."id",
"realograms_implementation_scan"."provided_values",
jsonb_build_array((
  SELECT jsonb_build_object('file_id', U0."file_id", 'type', U0."file_type") AS "a"
  FROM "realograms_implementation_scanfile" U0
  WHERE U0."scan_id" = "realograms_implementation_scan"."id"
)) AS "scan_files",
"master_data_implementation_category"."name" AS "selected_category_name",
"realograms_implementation_scan"."selected_category_id" AS "category_id",
"planograms_compliance_planogramcompliancereport"."section_name" AS "section_name"
FROM "realograms_implementation_scan"
LEFT OUTER JOIN "master_data_implementation_category"
ON ("realograms_implementation_scan"."selected_category_id" = "master_data_implementation_category"."id")
LEFT OUTER JOIN "realograms_implementation_realogram"
ON ("realograms_implementation_scan"."active_realogram_id" = "realograms_implementation_realogram"."id")
LEFT OUTER JOIN "planograms_compliance_planogramcompliancereport"
ON ("realograms_implementation_realogram"."id" = "planograms_compliance_planogramcompliancereport"."realogram_id")
WHERE "realograms_implementation_scan"."id" IN ({})
ORDER BY "realograms_implementation_scan"."id", "planograms_compliance_planogramcompliancereport"."id" NULLS LAST;
"""

def get_info_about_scans(scan_ids: typing.Sequence[int], *, instance_name: str, db_password: str) -> tuple[dict]:
    return run_sql(
        SQL_FOR_GETTING_INFO_ABOUT_SCANS.format(','.join(map(str, scan_ids))),
        instance_name=instance_name,
        db_password=db_password,
    )

def download_file(file_id: int, *, instance_name: str, auth_token: str) -> tuple[str, bytes]:
    """Download file from API and return filename and content"""
    response = requests.get(
        f'https://{instance_name}.rebotics.net/api/v1/master-data/file-upload/{file_id}/',
        headers={'Authorization': f'Token {auth_token}'},
        timeout=60
    )
    response.raise_for_status()
    raw_data = response.json()
    
    if raw_data is None:
        print(f"[DOWNLOAD] [ERROR] API returned None response for file {file_id}!", flush=True)
        raise ValueError(f"API returned None response for file {file_id}")
    
    if 'file' not in raw_data or 'original_filename' not in raw_data:
        print(f"[DOWNLOAD] [ERROR] API response missing required fields for file {file_id}!", flush=True)
        raise ValueError(f"API response missing required fields. Response: {raw_data}")
    
    file_url, file_name = raw_data['file'], raw_data['original_filename']
    
    response = requests.get(file_url, timeout=120)
    response.raise_for_status()
    
    return file_name, response.content

def download_file_threaded(file_id: int, file_name: str, save_path: str, instance_name: str, auth_token: str) -> tuple[int, str]:
    """Thread-safe wrapper for download_file function"""
    try:
        _, file_content = download_file(file_id, instance_name=instance_name, auth_token=auth_token)
        
        # Save file to disk
        with open(save_path, 'wb') as f:
            f.write(file_content)
        
        return file_id, save_path
    except Exception as e:
        print(f"[ERROR] [Thread] Error downloading file {file_id} to {save_path}: {e}", flush=True)
        raise

def sanitize_filename(filename: str) -> str:
    """Sanitize filename to remove invalid characters"""
    invalid_chars = '<>:"/\\|?*'
    for char in invalid_chars:
        filename = filename.replace(char, '_')
    # Remove leading/trailing spaces and dots
    filename = filename.strip('. ')
    return filename

def generate_filename(scan_id: int, section_name: typing.Optional[str], 
                     store_pog_id: typing.Optional[int], original_filename: str) -> str:
    """Generate filename with format: scan_id_section_name_storepog_id.ext
    If no section and store POG, then: scan_id.ext"""
    # Get file extension from original filename
    file_ext = os.path.splitext(original_filename)[1] or '.jpg'
    
    # Build filename parts
    parts = []
    
    # Always add scan ID first
    parts.append(str(scan_id))
    
    # Add section name if available
    if section_name:
        sanitized_section = sanitize_filename(str(section_name))
        parts.append(sanitized_section)
    
    # Add store POG ID if available
    if store_pog_id:
        parts.append(str(store_pog_id))
    
    # Join parts and add extension
    filename = "_".join(parts) + file_ext
    
    return filename

def process_batch_downloads(
    batch_number: int,
    batch_scans: list,
    download_folder: str,
    instance_name: str,
    auth_token: str
) -> tuple[int, int]:
    """
    Process a single batch of downloads.
    Returns: (successful_downloads, failed_downloads)
    """
    successful_downloads = 0
    failed_downloads = 0
    
    # Collect all files to download for this batch
    files_to_download = []
    for scan_info in batch_scans:
        source_scan_id = scan_info.get('id')
        section_name = scan_info.get('section_name')
        scan_files = scan_info.get('scan_files', [])
        provided_values = scan_info.get('provided_values')
        
        # Extract store POG ID from provided_values
        store_pog_id = None
        if provided_values:
            try:
                if isinstance(provided_values, dict):
                    # Check if _raw_data exists
                    if '_raw_data' in provided_values and isinstance(provided_values['_raw_data'], dict):
                        raw_data = provided_values['_raw_data']
                        # Try different possible field names for store POG ID
                        store_pog_id = raw_data.get('store_planogram') or raw_data.get('store_planogram_id')
                    elif 'store_planogram' in provided_values:
                        store_pog_id = provided_values.get('store_planogram') or provided_values.get('store_planogram_id')
            except Exception as e:
                print(f"[BATCH {batch_number}] [WARNING] Error extracting store POG ID for scan {source_scan_id}: {e}", flush=True)
        
        if not scan_files or scan_files is None:
            print(f"[BATCH {batch_number}] [WARNING] No scan_files for scan {source_scan_id}, skipping", flush=True)
            continue
        
        for file_index, scan_file in enumerate(scan_files, 1):
            if not scan_file or 'file_id' not in scan_file:
                continue
            
            file_id = scan_file['file_id']
            original_filename = scan_file.get('type', 'image.jpg')
            
            # Generate filename
            filename = generate_filename(
                scan_id=source_scan_id,
                section_name=section_name,
                store_pog_id=store_pog_id,
                original_filename=original_filename
            )
            
            save_path = os.path.join(download_folder, filename)
            files_to_download.append((file_id, filename, save_path, source_scan_id))
    
    if not files_to_download:
        print(f"[BATCH {batch_number}] No files to download in this batch", flush=True)
        return 0, 0
    
    print(f"[BATCH {batch_number}] Starting download of {len(files_to_download)} files", flush=True)
    
    # Download files with threading
    max_workers = min(20, len(files_to_download))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        download_futures = {
            executor.submit(download_file_threaded, file_id, filename, save_path, instance_name, auth_token): (file_id, source_scan_id)
            for file_id, filename, save_path, source_scan_id in files_to_download
        }
        
        completed_downloads = 0
        for future in as_completed(download_futures):
            try:
                file_id, saved_path = future.result()
                successful_downloads += 1
                completed_downloads += 1
                
                if completed_downloads % max(1, min(50, len(files_to_download) // 10)) == 0 or completed_downloads == len(files_to_download):
                    print(f"[BATCH {batch_number}] [DOWNLOAD] {completed_downloads}/{len(files_to_download)} ({completed_downloads*100//len(files_to_download)}%)", flush=True)
            except Exception as e:
                file_id, source_scan_id = download_futures[future]
                failed_downloads += 1
                print(f"[BATCH {batch_number}] [ERROR] Failed to download file {file_id} for scan {source_scan_id}: {e}", flush=True)
    
    print(f"[BATCH {batch_number}] Downloaded {successful_downloads}/{len(files_to_download)} files successfully", flush=True)
    return successful_downloads, failed_downloads

def run(*,
        from_instance: str,
        first_db_password: str,
        source_username: str,
        source_password: str,
        scan_ids_for_downloading: typing.Sequence[int],
        download_folder: str,
        batch_size: int = 10) -> None:
    try:
        print('Obtaining authentication token...', flush=True)
        _, auth_token = get_auth_token(from_instance, source_username, source_password)
        print(f"Auth token obtained for {from_instance}", flush=True)
        
        print('Getting info about scans...', flush=True)
        scans_info = get_info_about_scans(scan_ids_for_downloading, instance_name=from_instance, db_password=first_db_password)
        print(f"Retrieved info for {len(scans_info)} scans", flush=True)
        
        # Create download folder if it doesn't exist
        if not os.path.exists(download_folder):
            os.makedirs(download_folder, exist_ok=True)
            print(f"Created download folder: {download_folder}", flush=True)
        else:
            print(f"Using existing download folder: {download_folder}", flush=True)
        
        total_scans = len(scans_info)
        print(f"[BATCH] Processing scans in groups of {batch_size} (total scans: {total_scans})", flush=True)
        
        total_successful = 0
        total_failed = 0
        
        # Process batches
        total_batches = (total_scans + batch_size - 1) // batch_size
        for batch_start in range(0, total_scans, batch_size):
            batch_number = (batch_start // batch_size) + 1
            batch_scans = scans_info[batch_start:batch_start + batch_size]
            batch_count = len(batch_scans)
            
            # Extract source scan IDs for this batch
            batch_source_scan_ids = [scan_info.get('id') for scan_info in batch_scans if scan_info.get('id')]
            
            # Print batch start with scan IDs
            print(f"\n{'='*80}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Starting batch {batch_number}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Processing scans {batch_start + 1}-{batch_start + batch_count} of {total_scans}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Source Scan IDs: {', '.join(map(str, batch_source_scan_ids))}", flush=True)
            print(f"{'='*80}\n", flush=True)
            
            # Process batch downloads
            successful, failed = process_batch_downloads(
                batch_number=batch_number,
                batch_scans=batch_scans,
                download_folder=download_folder,
                instance_name=from_instance,
                auth_token=auth_token
            )
            
            total_successful += successful
            total_failed += failed
            
            # Print batch completion summary
            print(f"\n{'='*80}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Batch {batch_number} completed", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Source Scan IDs: {', '.join(map(str, batch_source_scan_ids))}", flush=True)
            print(f"[BATCH {batch_number}/{total_batches}] Success: {successful}, Failed: {failed}", flush=True)
            print(f"{'='*80}\n", flush=True)
        
        print(f"[SUCCESS] Downloaded {total_successful} files successfully across all batches", flush=True)
        print(f"[STATS] Failed downloads: {total_failed}", flush=True)
        print(f"[STATS] Download folder: {download_folder}", flush=True)
        print('Script completed successfully!', flush=True)
        
    except Exception as e:
        print(f"Error occurred: {e}", flush=True)
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Download scan images with category and section naming')
    parser.add_argument('--download-folder', type=str, default=None,
                        help='Folder path to download images (default: ./downloaded_images)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Number of scans to process per batch (default: 10)')
    
    args = parser.parse_args()
    
    # Use command line argument if provided, otherwise use default or config
    download_folder = args.download_folder
    if download_folder is None:
        # Try to get from config if available, otherwise use default
        try:
            download_folder = getattr(config, 'DOWNLOAD_FOLDER', './downloaded_images')
        except:
            download_folder = './downloaded_images'
    
    # Convert to absolute path
    download_folder = os.path.abspath(download_folder)
    
    run(
        from_instance=SOURCE_INSTANCE,
        first_db_password=SOURCE_DB_PASSWORD,
        source_username=SOURCE_USERNAME,
        source_password=SOURCE_PASSWORD,
        scan_ids_for_downloading=SCAN_IDS_FOR_COPYING,
        download_folder=download_folder,
        batch_size=args.batch_size,
    )

