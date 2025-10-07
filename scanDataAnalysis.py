#!/usr/bin/env python3
"""
Comprehensive scan data analysis script
Gets complete scan details from all relevant tables and creates CSV mapping
"""

import psycopg
from psycopg.rows import dict_row
import pandas as pd
import json
import os
from datetime import datetime
from config import *

def get_scan_data(instance_name: str, db_password: str, scan_ids: tuple):
    """Get complete scan data from all relevant tables"""
    
    print(f"Getting scan data from {instance_name}...")
    
    with psycopg.connect(
            user='proxyuser',
            password=db_password,
            host=f'{instance_name}-maint.rebotics.net',
            port='5432',
            dbname=instance_name,
            row_factory=dict_row
    ) as connection:
        with connection.cursor() as cursor:
            
            scan_ids_str = ','.join(map(str, scan_ids))
            
            # Main query to get all scan details (one row per scan)
            query = f"""
                SELECT 
                    s."id" as scan_id,
                    s."store_id",
                    s."created_at" as scan_created_at,
                    s."selected_category_id" as category_id,
                    cat."name" as category_name,
                    
                    -- Realogram details
                    r."id" as realogram_id,
                    rb."id" as realogram_bay_id,
                    
                    -- Store details
                    store."id" as store_id,
                    store."name" as store_name,
                    
                    -- Planogram compliance report details
                    pcr."id" as compliance_report_id,
                    pcr."section_id",
                    pcr."section_name",
                    pcr."store_planogram_id",
                    pcr."planogram_id",
                    pcr."aisle_name",
                    pcr."compliance_rate" as pog_percentage,
                    pcr."facing_compliance_rate",
                    pcr."sequence_compliance_rate",
                    pcr."initial_pre_compliance",
                    pcr."compliance_rates" as compliance_rates_json,
                    
                    -- Store planogram compliance (pre/post data)
                    spc."post_osa",
                    spc."pre_osa",
                    spc."post_compliance",
                    spc."pre_compliance",
                    spc."facing_compliance",
                    spc."sequence_post_compliance",
                    spc."sequence_pre_compliance",
                    spc."initial_pre_compliance" as spc_initial_pre_compliance,
                    
                    -- Planogram details
                    p."id" as planogram_id,
                    p."name" as planogram_name,
                    
                    -- Planogram counts for this scan
                    (SELECT COUNT(DISTINCT pcr2."planogram_id") 
                     FROM "planograms_compliance_planogramcompliancereport" pcr2
                     WHERE pcr2."realogram_id" = r."id") as planogram_unique_count,
                    (SELECT COUNT(pcr2."planogram_id") 
                     FROM "planograms_compliance_planogramcompliancereport" pcr2
                     WHERE pcr2."realogram_id" = r."id") as planogram_all_count,
                    
                    -- Realogram counts for this scan
                    (SELECT COUNT(DISTINCT r2."id") 
                     FROM "realograms_implementation_realogram" r2
                     WHERE r2."id" = s."active_realogram_id") as realogram_unique_count,
                    (SELECT COUNT(r2."id") 
                     FROM "realograms_implementation_realogram" r2
                     WHERE r2."id" = s."active_realogram_id") as realogram_all_count,
                    
                    -- Majority v2 logs data
                    mv2."id" as majority_v2_id,
                    mv2."store_planogram_id" as majority_store_planogram_id,
                    mv2."scan_created_date" as majority_created_at,
                    mv2."data" as majority_data
                    
                FROM "realograms_implementation_scan" s
                
                -- Category
                LEFT JOIN "master_data_implementation_category" cat
                    ON s."selected_category_id" = cat."id"
                
                -- Realogram
                LEFT JOIN "realograms_implementation_realogram" r
                    ON s."active_realogram_id" = r."id"
                
                -- Realogram Bay
                LEFT JOIN "realograms_implementation_realogrambay" rb
                    ON r."id" = rb."realogram_id"
                
                -- Store
                LEFT JOIN "master_data_implementation_store" store
                    ON s."store_id" = store."id"
                
                -- Planogram compliance report
                LEFT JOIN "planograms_compliance_planogramcompliancereport" pcr
                    ON r."id" = pcr."realogram_id"
                
                -- Store planogram compliance (pre/post data)
                LEFT JOIN "planograms_compliance_storeplanogramcompliance" spc
                    ON pcr."store_planogram_id" = spc."store_planogram_id"
                    AND pcr."scan_created_date" = spc."date"
                
                -- Planogram
                LEFT JOIN "planograms_implementation_planogram" p
                    ON pcr."planogram_id" = p."id"
                
                -- Majority v2 logs (join on store_planogram_id, get the latest record)
                LEFT JOIN LATERAL (
                    SELECT mv2."id", mv2."store_planogram_id", mv2."scan_created_date", mv2."data"
                    FROM "planograms_compliance_majorityv2log" mv2
                    WHERE mv2."store_planogram_id" = pcr."store_planogram_id"
                    ORDER BY mv2."scan_created_date" DESC
                    LIMIT 1
                ) mv2 ON true
                
                WHERE s."id" IN ({scan_ids_str})
                ORDER BY s."id";
            """
            
            print("Executing main query...")
            cursor.execute(query)
            results = cursor.fetchall()
            
            return results

def check_additional_sections(instance_name: str, db_password: str, scan_ids: tuple):
    """Check if sections are additional by looking at planogram section product data"""
    
    with psycopg.connect(
            user='proxyuser',
            password=db_password,
            host=f'{instance_name}-maint.rebotics.net',
            port='5432',
            dbname=instance_name,
            row_factory=dict_row
    ) as connection:
        with connection.cursor() as cursor:
            
            scan_ids_str = ','.join(map(str, scan_ids))
            
            # Query to check for additional sections
            query = f"""
                SELECT DISTINCT
                    s."id" as scan_id,
                    pcr."section_id",
                    pcr."section_name",
                    CASE 
                        WHEN EXISTS (
                            SELECT 1 FROM "planograms_implementation_planogramstore" sp
                            JOIN "planograms_implementation_planogram" p ON sp."planogram_id" = p."id"
                            JOIN "planograms_implementation_planogramsection" ps ON p."id" = ps."planogram_id"
                            JOIN "planograms_implementation_planogramsectionproduct" psp ON ps."id" = psp."section_id"
                            WHERE sp."id" = pcr."store_planogram_id"
                            AND psp."section_id" = pcr."section_id"
                            AND (psp."action" ILIKE '%additional%' OR psp."merch_method" ILIKE '%additional%')
                        ) THEN true
                        ELSE false
                    END as is_additional_section
                FROM "realograms_implementation_scan" s
                LEFT JOIN "realograms_implementation_realogram" r ON s."active_realogram_id" = r."id"
                LEFT JOIN "planograms_compliance_planogramcompliancereport" pcr ON r."id" = pcr."realogram_id"
                WHERE s."id" IN ({scan_ids_str})
                ORDER BY s."id";
            """
            
            cursor.execute(query)
            return cursor.fetchall()

def process_scan_data(scan_data, additional_sections_data):
    """Process and structure the scan data"""
    
    processed_data = []
    
    # Create a lookup for additional sections
    additional_sections_lookup = {}
    for row in additional_sections_data:
        key = f"{row['scan_id']}_{row['section_id']}"
        additional_sections_lookup[key] = row['is_additional_section']
    
    for row in scan_data:
        # Parse compliance rates JSON
        compliance_rates = {}
        if row['compliance_rates_json']:
            try:
                compliance_rates = json.loads(row['compliance_rates_json']) if isinstance(row['compliance_rates_json'], str) else row['compliance_rates_json']
            except:
                compliance_rates = {}
        
        # Extract map_by value for this specific scan from majority_data JSONB
        map_by_value = None
        if row['majority_data']:
            try:
                majority_data = row['majority_data'] if isinstance(row['majority_data'], dict) else json.loads(row['majority_data'])
                
                # The majority_data structure has scan IDs as keys, each with a map_by field
                # Look for the specific scan ID in the data
                scan_id_str = str(row['scan_id'])
                if scan_id_str in majority_data and isinstance(majority_data[scan_id_str], dict):
                    if 'map_by' in majority_data[scan_id_str]:
                        map_by_value = majority_data[scan_id_str]['map_by']
            except Exception as e:
                print(f"Error extracting map_by for scan {row['scan_id']}: {e}")
                map_by_value = None
        
        # Check if section is additional
        key = f"{row['scan_id']}_{row['section_id']}"
        is_additional_section = additional_sections_lookup.get(key, False)
        
        # Extract POG and OSA percentages
        pog_percentage = row['pog_percentage'] or 0
        osa_percentage = compliance_rates.get('osa', 0)
        
        # Pre/Post data
        pre_osa = row['pre_osa'] or 0
        post_osa = row['post_osa'] or 0
        pre_compliance = row['pre_compliance'] or 0
        post_compliance = row['post_compliance'] or 0
        
        processed_row = {
            'scan_id': row['scan_id'],
            'store_id': row['store_id'],
            'store_name': row['store_name'],
            'scan_created_at': row['scan_created_at'],
            'category_id': row['category_id'],
            'category_name': row['category_name'],
            'realogram_id': row['realogram_id'],
            'realogram_bay_id': row['realogram_bay_id'],
            'compliance_report_id': row['compliance_report_id'],
            'section_id': row['section_id'],
            'section_name': row['section_name'],
            'store_planogram_id': row['store_planogram_id'],
            'planogram_id': row['planogram_id'],
            'planogram_name': row['planogram_name'],
            'aisle_name': row['aisle_name'],
            'is_additional_section': is_additional_section,
            'pog_percentage': pog_percentage,
            'osa_percentage': osa_percentage,
            'pre_osa': pre_osa,
            'post_osa': post_osa,
            'pre_compliance': pre_compliance,
            'post_compliance': post_compliance,
            'facing_compliance_rate': row['facing_compliance_rate'],
            'sequence_compliance_rate': row['sequence_compliance_rate'],
            'initial_pre_compliance': row['initial_pre_compliance'],
            'spc_initial_pre_compliance': row['spc_initial_pre_compliance'],
            'compliance_rates_json': json.dumps(compliance_rates) if compliance_rates else None,
            'planogram_unique_count': row['planogram_unique_count'],
            'planogram_all_count': row['planogram_all_count'],
            'realogram_unique_count': row['realogram_unique_count'],
            'realogram_all_count': row['realogram_all_count'],
            'majority_v2_id': row['majority_v2_id'],
            'majority_store_planogram_id': row['majority_store_planogram_id'],
            'majority_created_at': row['majority_created_at'],
            'majority_data': json.dumps(row['majority_data']) if row['majority_data'] else None,
            'map_by_value': map_by_value
        }
        
        processed_data.append(processed_row)
    
    return processed_data

def create_csv_report(processed_data, filename_prefix="scan_data_analysis"):
    """Create CSV report from processed data with only essential fields"""
    
    if not processed_data:
        print("No data to create CSV report")
        return None
    
    # Create DataFrame
    df = pd.DataFrame(processed_data)
    
    # Select only the essential columns
    essential_columns = [
        'scan_id',
        'store_name', 
        'category_id',
        'category_name',
        'store_planogram_id',
        'planogram_name',
        'section_id',
        'section_name',
        'pre_compliance',  # This is the PRE POG
        'post_compliance', # This is the POST POG
        'pre_osa',         # This is the PRE OSA
        'post_osa'         # This is the POST OSA
    ]
    
    # Filter DataFrame to only include essential columns
    df_filtered = df[essential_columns].copy()
    
    # Rename columns for clarity
    df_filtered = df_filtered.rename(columns={
        'pre_compliance': 'pre_pog_percentage',
        'post_compliance': 'post_pog_percentage',
        'pre_osa': 'pre_osa_percentage',
        'post_osa': 'post_osa_percentage'
    })
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"
    
    # Save to CSV
    df_filtered.to_csv(filename, index=False)
    print(f"CSV report saved as: {filename}")
    
    # Display summary
    print(f"\n=== SUMMARY ===")
    print(f"Total records: {len(df_filtered)}")
    print(f"Unique scan IDs: {df_filtered['scan_id'].nunique()}")
    print(f"Unique sections: {df_filtered['section_id'].nunique()}")
    
    # Show sample data
    print(f"\n=== SAMPLE DATA ===")
    print(df_filtered.head())
    
    return filename

def get_scans_by_filters(instance_name: str, db_password: str, start_date=None, end_date=None, store_id=None):
    """Get scan IDs and store IDs filtered by status and report conditions"""
    
    print(f"Getting scans from {instance_name} with filters...")
    
    with psycopg.connect(
            user='proxyuser',
            password=db_password,
            host=f'{instance_name}-maint.rebotics.net',
            port='5432',
            dbname=instance_name,
            row_factory=dict_row
    ) as connection:
        with connection.cursor() as cursor:
            
            # Build WHERE conditions
            where_conditions = []
            params = []
            
            # Status condition
            where_conditions.append('s."status" = %s')
            params.append('done')
            
            # Has report condition
            where_conditions.append('s."has_report" = %s')
            params.append(True)
            
            # Date conditions
            if start_date:
                where_conditions.append('s."created_at"::date >= %s')
                params.append(start_date)
            
            if end_date:
                where_conditions.append('s."created_at"::date <= %s')
                params.append(end_date)
            
            # Store ID condition
            if store_id:
                where_conditions.append('s."store_id" = %s')
                params.append(store_id)
            
            # Build the query
            where_clause = ' AND '.join(where_conditions)
            
            query = f"""
                SELECT 
                    s."id" as scan_id,
                    s."store_id",
                    s."created_at",
                    s."status",
                    s."has_report"
                FROM "realograms_implementation_scan" s
                WHERE {where_clause}
                ORDER BY s."created_at" DESC;
            """
            
            print(f"Executing query with conditions: {where_conditions}")
            cursor.execute(query, params)
            results = cursor.fetchall()
            
            return results

def main():
    """Main function"""
    
    print("=" * 60)
    print("COMPREHENSIVE SCAN DATA ANALYSIS")
    print("=" * 60)
    
    try:
        # Get source data
        print(f"\n{'='*20} SOURCE DATABASE ({SOURCE_INSTANCE}) {'='*20}")
        source_data = get_scan_data(SOURCE_INSTANCE, SOURCE_DB_PASSWORD, SCAN_IDS_FOR_COPYING)
        print(f"Retrieved {len(source_data)} records from source database")
        
        # Check for additional sections
        print("Checking for additional sections...")
        additional_sections_data = check_additional_sections(SOURCE_INSTANCE, SOURCE_DB_PASSWORD, SCAN_IDS_FOR_COPYING)
        print(f"Retrieved {len(additional_sections_data)} additional section records")
        
        # Process data
        processed_data = process_scan_data(source_data, additional_sections_data)
        print(f"Processed {len(processed_data)} records")
        
        # Create CSV report
        csv_filename = create_csv_report(processed_data, "source_scan_data")
        
        print(f"\nAnalysis complete! Check the CSV file: {csv_filename}")
        
        # Example: Get scans with filters
        print(f"\n{'='*20} EXAMPLE: GET SCANS WITH FILTERS {'='*20}")
        print("Getting scans with status='done' and has_report=true...")
        
        # You can modify these parameters as needed
        filtered_scans = get_scans_by_filters(
            SOURCE_INSTANCE, 
            SOURCE_DB_PASSWORD,
            start_date='2025-08-01',  # Modify as needed
            end_date='2025-08-31',    # Modify as needed
            store_id=8                # Modify as needed
        )
        
        print(f"Found {len(filtered_scans)} scans matching criteria")
        
        if filtered_scans:
            print("\nSample filtered scans:")
            for scan in filtered_scans[:5]:  # Show first 5
                print(f"  Scan ID: {scan['scan_id']}, Store ID: {scan['store_id']}, Created: {scan['created_at']}")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

def create_detailed_csv_report(processed_data, filename_prefix="scan_details", output_folder=None):
    """Create detailed CSV report with scan status and all required fields"""
    
    if not processed_data:
        print("No data to create CSV report")
        return None
    
    # Create DataFrame
    df = pd.DataFrame(processed_data)
    
    # Select detailed columns including scan status
    detailed_columns = [
        'scan_id',
        'store_planogram_id',
        'planogram_name',
        'section_id',
        'section_name',
        'is_additional_section',
        'pre_compliance',  # PRE POG
        'post_compliance', # POST POG
        'pre_osa',         # PRE OSA
        'post_osa',        # POST OSA
        'compliance_rates_json',  # JSONB data for counts
        'planogram_unique_count',  # Unique planogram count
        'planogram_all_count',     # Total planogram count
        'realogram_unique_count',  # Unique realogram count
        'realogram_all_count',     # Total realogram count
        'majority_v2_id',  # Majority v2 logs ID
        'majority_store_planogram_id',  # Store POG ID from majority v2
        'majority_created_at',  # Majority v2 creation timestamp
        'majority_data',  # Majority v2 data
        'map_by_value'  # Map by value from majority v2 data for this scan
    ]
    
    # Filter DataFrame to only include detailed columns
    df_filtered = df[detailed_columns].copy()
    
    # Rename columns for clarity
    df_filtered = df_filtered.rename(columns={
        'pre_compliance': 'pre_pog_percentage',
        'post_compliance': 'post_pog_percentage',
        'pre_osa': 'pre_osa_percentage',
        'post_osa': 'post_osa_percentage'
    })
    
    # Add scan status column (assuming all scans are processed)
    df_filtered['scan_status'] = 'processed'
    
    # Extract counts from JSONB data
    df_filtered['ok_count'] = 0
    df_filtered['wandering_count'] = 0
    df_filtered['oos_count'] = 0
    df_filtered['hole_count'] = 0
    
    for idx, row in df_filtered.iterrows():
        if row['compliance_rates_json']:
            try:
                import json
                rates = json.loads(row['compliance_rates_json']) if isinstance(row['compliance_rates_json'], str) else row['compliance_rates_json']
                
                # Extract counts from JSON
                df_filtered.at[idx, 'ok_count'] = rates.get('correct', 0)
                df_filtered.at[idx, 'wandering_count'] = rates.get('wandering', 0)
                df_filtered.at[idx, 'oos_count'] = rates.get('missing', 0)  # OOS = missing items
                df_filtered.at[idx, 'hole_count'] = rates.get('empty', 0)   # Holes = empty spaces
                
            except Exception as e:
                print(f"Warning: Could not parse compliance_rates_json for scan {row['scan_id']}: {e}")
                # Keep default values of 0
    
    # Remove the JSONB column as we've extracted the needed data
    df_filtered = df_filtered.drop('compliance_rates_json', axis=1)
    
    # Reorder columns
    column_order = [
        'scan_id',
        'scan_status',
        'store_planogram_id',
        'planogram_name',
        'section_id',
        'section_name',
        'is_additional_section',
        'pre_pog_percentage',
        'post_pog_percentage',
        'pre_osa_percentage',
        'post_osa_percentage',
        'ok_count',
        'wandering_count',
        'oos_count',
        'hole_count',
        'planogram_unique_count',
        'planogram_all_count',
        'realogram_unique_count',
        'realogram_all_count',
        'majority_v2_id',
        'majority_store_planogram_id',
        'majority_created_at',
        'majority_data',
        'map_by_value'
    ]
    
    df_filtered = df_filtered[column_order]
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"
    
    # Use output folder if provided
    if output_folder:
        filename = os.path.join(output_folder, filename)
    
    # Save to CSV
    df_filtered.to_csv(filename, index=False)
    print(f"CSV report saved as: {filename}")
    
    # Display summary
    print(f"\n=== SUMMARY ===")
    print(f"Total records: {len(df_filtered)}")
    print(f"Unique scan IDs: {df_filtered['scan_id'].nunique()}")
    print(f"Additional sections: {df_filtered['is_additional_section'].sum()}")
    
    # Show sample data
    print(f"\n=== SAMPLE DATA ===")
    print(df_filtered.head())
    
    return filename

if __name__ == "__main__":
    main()
