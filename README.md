# Scan Copy SDK

A comprehensive Python SDK for copying scans between database instances, downloading scan images, and generating analysis reports with batch processing, checkpointing, and retry mechanisms.

## Features

### Core Functionalities

- 🔄 **Scan Copying**: Copy scans from source to target database instances with batch processing
- 📥 **Image Downloading**: Download scan images with intelligent naming based on scan ID, section name, and store POG ID
- 📊 **Data Analysis**: Comprehensive scan data analysis with POG and OSA metrics (optional)
- 🎨 **Highlighted Reports**: Color-coded Excel reports for easy identification of differences
- 📁 **Custom Results Path**: Choose where to save test results
- ⚙️ **Interactive Workflow**: Step-by-step guided process with functionality selection

### Advanced Features

- 🔁 **Batch Processing**: Process scans in configurable batches (default: 10 scans per batch)
- 💾 **Checkpointing**: Automatic progress saving with resume capability
- 🔄 **Retry Logic**: Exponential backoff retry mechanism for failed operations
- 🧵 **Multi-threading**: Concurrent file downloads/uploads for improved performance
- ⏱️ **Extended Timeouts**: 30-minute timeout support for large batch operations
- 📝 **Real-time Logging**: Live console output with progress tracking
- 🛡️ **Error Handling**: Comprehensive error handling with detailed logging

## Installation

### Prerequisites
- Python 3.8 or higher
- Access to source and target database instances
- Network access to database hosts and API endpoints

### Steps

1. **Clone the repository:**
   ```bash
   git clone https://github.com/retech-us/MATESTS.git
   cd MATESTS
   ```

2. **Set up configuration files:**
   ```bash
   # Copy template files (these contain no sensitive data)
   cp config.py.template config.py
   cp config.json.template config.json
   ```

3. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the SDK:**
   ```bash
   python createScansSDK.py
   ```

## Usage

### Quick Start

1. **Run the SDK**: `python createScansSDK.py`
2. **Choose functionality**: Select between Copy Scan or Download Images
3. **Follow the interactive prompts** to configure your workflow
4. **Review generated reports** in the results folder

## Workflows

### Workflow 1: Copy Scan

The Copy Scan workflow allows you to copy scans from a source instance to a target instance with optional analysis.

#### Step 0: Choose Functionality
- Select option 1 for Copy Scan

#### Step 1: Configuration
Enter database credentials:
- Source Database Instance
- Source Database Password
- Source Username
- Source Password
- Target Database Instance
- Target Database Password
- Target Username
- Target Password

#### Step 1.5: Results Path
Choose where to save results:
- Option 1: Default path (`./testResults/`)
- Option 2: Custom path (specify your own directory)

#### Step 2: Source Scan IDs
Enter comma-separated scan IDs to copy:
- Can use existing scan IDs from config if available
- Or enter new scan IDs manually

#### Step 3: Target Store
Enter the target store ID where scans will be copied

#### Step 4: Run Copy Script
Choose which copy script to use:
- Option 1: `mappedScans.py` (mapped scans script)
- Option 2: `autoScans.py` (auto scans script with enhanced features)

**Checkpoint Handling:**
- If checkpoint files are detected, you'll be prompted to:
  - Resume from checkpoint (continues from last completed batch)
  - Restart from scratch (deletes checkpoint and starts fresh)

**Batch Processing:**
- Scans are processed in batches of 10 (configurable)
- Each batch includes:
  - File downloads (concurrent, up to 20 workers)
  - File uploads (concurrent, up to 20 workers)
  - Scan creation (concurrent, up to 10 workers)
- Progress is saved after each batch completion
- Failed batches are automatically retried (up to 3 retries)

**Real-time Progress:**
- Batch start/completion logs with scan IDs
- Download/upload progress percentages
- Success/failure counts per batch
- Estimated remaining time

#### Step 4.5: Analysis Option
After copying completes, choose whether to run analysis:
- Option Y: Continue with analysis (proceeds to Step 5)
- Option N: End workflow without analysis

#### Step 5: Get Target Scan IDs
- Automatically extracts target scan IDs from mapping CSV if available
- Or manually enter target scan IDs for analysis

#### Step 6: Generate Analysis
- Creates comprehensive CSV and Excel reports
- Includes color-coded highlighting for differences
- Generates source and target scan analysis files

### Workflow 2: Download Images

The Download Images workflow downloads scan images with intelligent naming.

#### Step 0: Choose Functionality
- Select option 2 for Download Images

#### Step 1: Configuration
Enter source database credentials:
- Source Database Instance
- Source Database Password
- Source Username
- Source Password

#### Step 2: Source Scan IDs
Enter comma-separated scan IDs to download images from

#### Step 3: Download Folder
Choose download location:
- Option 1: Default path (`./downloaded_images/`)
- Option 2: Custom path

#### Step 4: Batch Download
Images are downloaded in batches with:
- Concurrent downloads (up to 20 workers per batch)
- Progress tracking per batch
- Automatic retry on failures

**File Naming Convention:**
- Format: `{scan_id}_{section_name}_{store_pog_id}.ext`
- If section and POG exist: `12345_SectionName_67890.jpg`
- If only section exists: `12345_SectionName.jpg`
- If only POG exists: `12345_67890.jpg`
- If neither exists: `12345.jpg`

## Batch Processing

### Overview

Both copy and download workflows use batch processing to handle large numbers of scans efficiently.

### Batch Configuration

- **Default Batch Size**: 10 scans per batch
- **Concurrent Workers**:
  - Downloads: 20 workers
  - Uploads: 20 workers
  - Scan Creation: 10 workers

### Batch Processing Features

1. **Automatic Batching**: Scans are automatically divided into batches
2. **Progress Tracking**: Real-time progress for each batch
3. **Error Isolation**: Failures in one batch don't stop other batches
4. **Retry Logic**: Failed batches are retried automatically
5. **Checkpointing**: Progress is saved after each batch

### Batch Retry Logic

Batches are retried if:
- Download success rate < 80%
- Upload success rate < 80%
- Scan creation success rate < 50%

Retry configuration:
- Maximum retries: 3 attempts per batch
- Exponential backoff: 5 seconds × retry attempt
- Total timeout: 30 minutes per batch

### Batch Logging

Each batch logs:
- Batch number and total batches
- Source scan IDs in the batch
- Download/upload progress
- Success/failure counts
- Completion status

Example log output:
```
================================================================================
[BATCH 1/5] Starting batch 1
[BATCH 1/5] Processing scans 1-10 of 50
[BATCH 1/5] Source Scan IDs: 12345, 12346, 12347, ...
================================================================================
[BATCH 1] Starting download of 25 files
[BATCH 1] [DOWNLOAD] 25/25 (100%)
[BATCH 1] Starting upload of 25 files
[BATCH 1] [UPLOAD] 25/25 (100%)
[BATCH 1] [SCAN] Creating 10 scans...
[BATCH 1] [SCAN] 10/10 (100%)
================================================================================
[BATCH 1/5] Batch 1 completed
[BATCH 1/5] Success: 10, Failed: 0
================================================================================
```

## Checkpointing and Resume

### Automatic Checkpointing

The system automatically saves progress after each batch:
- Checkpoint file: `checkpoint_{timestamp}.json`
- Contains: Completed batch numbers, scan mappings, failed scan count

### Resume Functionality

When restarting:
1. System detects existing checkpoint files
2. Prompts user to resume or restart
3. If resuming:
   - Skips already completed batches
   - Continues from next incomplete batch
   - Preserves existing scan mappings
4. If restarting:
   - Deletes checkpoint files
   - Starts from beginning

### Checkpoint File Structure

```json
{
  "completed_batches": [1, 2, 3],
  "scan_mapping": [
    {"source_scan_id": 12345, "target_scan_id": 67890},
    ...
  ],
  "failed_scans": 0
}
```

## Error Handling

### Retry Mechanisms

1. **API Retries**: Exponential backoff for 502/503 errors
   - Base delay: 1-2 seconds
   - Max delay: 60 seconds
   - Max retries: 3 attempts
   - Total timeout: 30 minutes

2. **Batch Retries**: Automatic retry for failed batches
   - Retry conditions based on success rates
   - Exponential backoff between retries
   - Maximum 3 retry attempts

3. **Individual Operation Retries**: Per-file and per-scan retries
   - Handles transient network errors
   - Timeout handling for long operations

### Error Logging

Detailed error logging includes:
- HTTP status codes (especially 400 Bad Request)
- API request payloads
- API response bodies
- File/scan IDs causing errors
- Stack traces for debugging

### Error Recovery

- Failed files don't block batch completion
- Failed scans are logged and tracked
- Partial success is allowed (continues with available data)
- Checkpoint saves progress even with some failures

## File Structure

```
MATESTS/
├── createScansSDK.py       # Main SDK with interactive workflow
├── mappedScans.py          # Mapped scans copying script
├── autoScans.py            # Auto scans copying script with batch processing
├── downloadScanImages.py   # Image download script with intelligent naming
├── scanDataAnalysis.py     # Analysis functions
├── config.py.template      # Configuration template (safe to commit)
├── config.json.template    # JSON configuration template (safe to commit)
├── requirements.txt        # Python dependencies
├── README.md               # This file
├── testResults/            # Default results directory
│   └── run_YYYYMMDD_HHMMSS/
│       ├── initial_scan_mapping_*.csv
│       ├── scan_mapping_updated_*.csv
│       ├── source_scandetails_*.csv
│       ├── target_scandetails_*.csv
│       ├── analysis_with_comments_*.csv
│       └── analysis_with_comments_*_highlighted.xlsx
└── downloaded_images/      # Default download directory
    └── {scan_id}_{section}_{pog}.ext
```

## Output Files

### Copy Scan Workflow Outputs

1. **Scan Mapping CSV**: Maps source scan IDs to target scan IDs
   - `initial_scan_mapping_YYYYMMDD_HHMMSS.csv`
   - `scan_mapping_updated_YYYYMMDD_HHMMSS.csv`

2. **Analysis CSV Files**:
   - `source_scandetails_YYYYMMDD_HHMMSS.csv`: Source scan analysis
   - `target_scandetails_YYYYMMDD_HHMMSS.csv`: Target scan analysis
   - `analysis_with_comments_YYYYMMDD_HHMMSS.csv`: Combined analysis

3. **Excel Report**:
   - `analysis_with_comments_YYYYMMDD_HHMMSS_highlighted.xlsx`: Color-coded Excel file

### Download Images Workflow Outputs

- Image files named: `{scan_id}_{section_name}_{store_pog_id}.ext`
- Files organized in the specified download folder
- Progress logs in console

## Color Coding (Analysis Reports)

The highlighted analysis uses the following color scheme:

- 🔴 **Red**: Wrong POG Name Mapping (different POG names between source and target)
- 🟠 **Orange**: Same POG Name but Different Section (same POG, different sections)
- 🟡 **Yellow**: Target Has Additional Section (source has no additional section, target does)
- 🔵 **Blue**: Target Has Higher POG% Than Source (target POG% > source POG%)
- 🟢 **Green**: No Issues (all mappings are correct)

## Configuration

### Database Configuration

The SDK prompts for:
- Source Database Instance
- Source Database Password
- Source Username
- Source Password
- Target Database Instance (for copy workflow)
- Target Database Password (for copy workflow)
- Target Username (for copy workflow)
- Target Password (for copy workflow)

### Security Features

- ✅ **Password Masking**: All passwords are hidden during input using `getpass`
- ✅ **No Hardcoded Credentials**: All configuration is collected interactively
- ✅ **Secure Storage**: Credentials are only stored temporarily in memory
- ✅ **No Password Echo**: Passwords are never displayed on screen
- ✅ **Config Updates**: Automatically updates `config.py` with current values

### Environment Configuration

The SDK automatically updates `config.py` with:
- Database credentials
- Scan IDs for copying/downloading
- Target store ID
- Other runtime values

## Performance Optimization

### Optimizations for Large Batches

1. **Extended Timeouts**:
   - API calls: 60-120 seconds
   - Total operation: 30 minutes
   - Prevents premature timeouts on large batches

2. **Concurrent Processing**:
   - Multiple threads for downloads/uploads
   - Parallel scan creation
   - Efficient resource utilization

3. **Batch Processing**:
   - Processes scans in manageable chunks
   - Reduces memory usage
   - Enables progress tracking

4. **Real-time Output**:
   - Flushed console output for immediate visibility
   - Progress indicators
   - Estimated remaining time

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify credentials and network access
   - Check database instance names
   - Ensure firewall allows connections

2. **Timeout Errors**
   - Timeouts are set to 30 minutes for large batches
   - Check network stability
   - Verify API endpoint availability

3. **Permission Errors**
   - Ensure write access to results directory
   - Check download folder permissions
   - Verify file system permissions

4. **Missing Dependencies**
   - Run `pip install -r requirements.txt`
   - Verify Python version (3.8+)
   - Check for missing system libraries

5. **Checkpoint Issues**
   - Checkpoint files can be manually deleted to restart
   - Verify JSON file integrity
   - Check disk space availability

6. **400 Bad Request Errors**
   - Check detailed error logs for request payload
   - Verify scan data structure
   - Review API response for specific errors

7. **NoneType Errors**
   - Check API responses for missing data
   - Verify scan information completeness
   - Review error logs for specific fields

### Error Messages

- **"Password authentication failed"**: Check database credentials
- **"Permission denied creating folder"**: Check directory permissions
- **"No scan mapping CSV file found"**: Ensure copy script completed successfully
- **"Checkpoint file corrupted"**: Delete checkpoint and restart
- **"Batch processing failed"**: Check error logs for specific batch issues
- **"API returned None response"**: Verify API endpoint and authentication

### Debugging Tips

1. **Enable Verbose Logging**: Check console output for detailed logs
2. **Review Checkpoint Files**: Inspect JSON files for progress state
3. **Check API Responses**: Review 400 error logs for payload issues
4. **Verify Scan Data**: Ensure scan IDs exist and have valid data
5. **Network Diagnostics**: Test connectivity to database and API endpoints

## Dependencies

### Required Python Packages

- `pandas>=1.5.0` - Data manipulation and analysis
- `numpy>=1.21.0` - Numerical computing
- `psycopg[binary]>=3.0.0` - PostgreSQL database adapter
- `requests>=2.28.0` - HTTP library for API calls
- `openpyxl>=3.0.0` - Excel file generation (optional, for Excel reports)

### Installation

```bash
pip install -r requirements.txt
```

## API Timeouts and Limits

### Timeout Configuration

- **Authentication**: 30 seconds
- **File Download (metadata)**: 60 seconds
- **File Download (content)**: 120 seconds
- **File Upload**: 120 seconds
- **Scan Creation**: 60 seconds
- **Total Batch Operation**: 30 minutes

### Rate Limiting

- Automatic retry with exponential backoff for rate limit errors
- Concurrent operations limited by worker counts
- Respects API rate limits through backoff mechanism

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly with various batch sizes
5. Update documentation if needed
6. Submit a pull request

## License

This project is part of the retech-us organization and follows the organization's licensing terms.

## Support

For support and questions:
- Create an issue in this repository
- Contact the development team
- Check the troubleshooting section above
- Review error logs for specific issues

## Changelog

### Recent Updates

- **Batch Processing**: Added configurable batch processing with progress tracking
- **Checkpointing**: Implemented automatic checkpointing with resume capability
- **Retry Logic**: Enhanced retry mechanisms with exponential backoff
- **Download Images**: New workflow for downloading images with intelligent naming
- **Analysis Option**: Made analysis optional after scan copying
- **Error Logging**: Improved error logging with detailed API payload information
- **Performance**: Optimized for handling 500+ scans with extended timeouts
- **Real-time Output**: Added real-time console output with progress indicators

---

**Note**: This SDK is designed for internal use within the retech-us organization for scan copying, image downloading, and analysis workflows. All operations include comprehensive error handling, checkpointing, and retry mechanisms to ensure reliable processing of large datasets.
