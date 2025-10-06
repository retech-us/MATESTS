# Scan Copy SDK

A comprehensive Python SDK for copying scans between database instances, analyzing scan data, and generating highlighted reports with clipboard support.

## Features

- 🔄 **Scan Copying**: Copy scans between source and target database instances
- 📊 **Data Analysis**: Comprehensive scan data analysis with POG and OSA metrics
- 🎨 **Highlighted Reports**: Color-coded Excel reports for easy identification of differences
- 📁 **Custom Results Path**: Choose where to save test results
- ⚙️ **Interactive Workflow**: Step-by-step guided process
- 📈 **Multiple Export Formats**: CSV and Excel output options
- 📁 **Organized Results**: Creates timestamped folders for each run's results
- 🔒 **Password Security**: All passwords are masked during input

## Security Features

- ✅ **Password Masking**: All passwords are hidden during input using `getpass`
- ✅ **No Hardcoded Credentials**: All configuration is collected interactively
- ✅ **Secure Storage**: Credentials are only stored temporarily in memory
- ✅ **No Password Echo**: Passwords are never displayed on screen

## Installation

### Prerequisites
- Python 3.8 or higher
- Access to source and target database instances

### Quick Install
```bash
# Clone or download the project files
# Navigate to the project directory
cd MATESTS

# Install dependencies
pip install -r requirements.txt
```

### Manual Installation
```bash
pip install pandas numpy psycopg[binary] requests pyperclip openpyxl
```

## Configuration

### Interactive Configuration
The SDK will prompt you for all required configuration details when you run it. No need to manually create configuration files!

**Required Information:**
- Source Database Instance (e.g., albt)
- Source Database Password
- Source Username
- Source Password
- Target Database Instance (e.g., stgalbt)
- Target Database Password
- Target Username
- Target Password

The SDK will automatically save your configuration to `config.json` and update `config.py` as needed.

## Usage

### Main SDK Workflow
```bash
python scanCopySDK.py
```

This will guide you through:
1. **Results Folder**: Creates timestamped folder for this run's results
2. **Configuration**: Enter database credentials interactively
3. **Source Scan IDs**: Enter comma-separated scan IDs to copy
4. **Target Store**: Enter target store ID
5. **Copy Script**: Choose between `copyScans.py` or `copyScanUpdated.py`
6. **Target Scan IDs**: Enter resulting target scan IDs

### Highlighted Analysis
The highlighted analysis is now integrated into the main SDK workflow. It will:
- Compare source and target scan data
- Generate color-coded Excel reports
- Provide clipboard copying options

### Direct Scan Data Analysis
```bash
python scanDataAnalysis.py
```

This will generate detailed scan analysis CSV files.

## File Structure

```
MATESTS/
├── scanCopySDK.py              # Main SDK workflow
├── scanDataAnalysis.py         # Scan data analysis
├── copyScans.py                # Original scan copying script
├── copyScanUpdated.py          # Updated scan copying script
├── config.py                   # Python configuration (auto-updated)
├── config.json                 # Database configuration
├── requirements.txt            # Python dependencies
├── README.md                   # This file
└── *.csv                       # Generated analysis files
```

## Color Coding

The highlighted analysis uses the following color scheme:

- 🔴 **Red**: Wrong POG Name Mapping (different POG names between source and target)
- 🟠 **Orange**: Same POG Name but Different Section (same POG, different sections)
- 🟡 **Yellow**: Target Has Additional Section (source has no additional section, target does)
- 🔵 **Blue**: Target Has Higher POG% Than Source (target POG% > source POG%)
- 🟢 **Green**: No Issues (all mappings are correct)

## Custom Results Path

The SDK allows you to choose where to save your test results:

### Path Options:
1. **Default Path**: Uses `./testResults/` in the current directory
2. **Custom Path**: Specify any valid directory path on your system

### Path Features:
- ✅ **Path Validation**: Automatically validates the chosen path
- ✅ **Directory Creation**: Creates directories if they don't exist
- ✅ **Absolute Paths**: Converts relative paths to absolute paths
- ✅ **Error Handling**: Clear error messages for invalid paths
- ✅ **Fallback System**: Falls back to default if custom path fails

## Output Files

The SDK generates organized results in timestamped folders:

```
testResults/
└── run_YYYYMMDD_HHMMSS/
    ├── scan_mapping_updated_YYYYMMDD_HHMMSS.csv
    ├── source_scandetails_YYYYMMDD_HHMMSS.csv
    ├── target_scandetails_YYYYMMDD_HHMMSS.csv
    ├── analysis_with_comments_YYYYMMDD_HHMMSS.csv
    └── analysis_with_comments_YYYYMMDD_HHMMSS_highlighted.xlsx
```

### File Descriptions:
- **scan_mapping_updated_*.csv**: Source to target scan ID mappings
- **source_scandetails_*.csv**: Detailed source scan data with counts
- **target_scandetails_*.csv**: Detailed target scan data with counts
- **analysis_with_comments_*.csv**: Combined analysis with difference comments
- **analysis_with_comments_*_highlighted.xlsx**: Color-coded Excel report

## Data Fields

The analysis includes the following key fields:

- **Scan Information**: scan_id, store_name, category_name
- **Planogram Data**: store_planogram_id, planogram_name
- **Section Data**: section_id, section_name, is_additional_section
- **Compliance Metrics**: pre_pog_percentage, post_pog_percentage
- **OSA Metrics**: pre_osa_percentage, post_osa_percentage
- **Item Counts**: 
  - `ok_count`: Count of correctly placed items
  - `wandering_count`: Count of items in wrong positions
  - `oos_count`: Count of out-of-stock items (missing)
  - `hole_count`: Count of empty spaces (holes)
- **Difference Flags**: Various boolean flags for identifying differences

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify database credentials in `config.json`
   - Check network connectivity to database instances

2. **Missing Dependencies**
   - Run `pip install -r requirements.txt`
   - Ensure Python 3.8+ is installed

3. **Clipboard Issues**
   - Ensure `pyperclip` is installed: `pip install pyperclip`
   - On Linux, may need additional packages for clipboard support

4. **Excel Export Issues**
   - Ensure `openpyxl` is installed: `pip install openpyxl`

### Error Messages

- `❌ Configuration not found`: Check `config.json` exists and is valid
- `❌ Database connection failed`: Verify credentials and connectivity
- `❌ No scan data found`: Check scan IDs exist in database
- `❌ Clipboard error`: Install pyperclip or check system clipboard

## Support

For issues or questions:
1. Check the troubleshooting section above
2. Verify all dependencies are installed
3. Check database connectivity and credentials
4. Review error messages for specific guidance

## License

This project is for internal use. Please ensure compliance with your organization's data handling policies.
