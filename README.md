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

### Steps
1. **Clone the repository:**
   ```bash
   git clone https://github.com/retech-us/MATESTS.git
   cd MATESTS
   ```

2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the SDK:**
   ```bash
   python scanCopySDK.py
   ```

## Usage

### Quick Start
1. **Run the SDK**: `python scanCopySDK.py`
2. **Follow the interactive prompts** to configure your database connections
3. **Enter scan IDs** you want to copy and analyze
4. **Choose target store** for the copied scans
5. **Select copy script** (copyScans.py or copyScanUpdated.py)
6. **Review generated reports** in the results folder

### Workflow Steps
1. **Configuration**: Enter database credentials and settings
2. **Results Path**: Choose where to save analysis results
3. **Source Scan IDs**: Enter comma-separated scan IDs to copy
4. **Target Store**: Enter target store ID
5. **Copy Scripts**: Choose between copyScans.py or copyScanUpdated.py
6. **Target Scan IDs**: Enter target scan IDs for analysis
7. **Analysis**: Generate comprehensive CSV and Excel reports

## File Structure

```
MATESTS/
├── scanCopySDK.py          # Main SDK with interactive workflow
├── copyScans.py            # Original scan copying script
├── copyScanUpdated.py      # Updated scan copying script
├── scanDataAnalysis.py     # Analysis functions
├── config.py               # Configuration template
├── requirements.txt        # Python dependencies
├── README.md               # This file
└── *.csv                   # Generated analysis files
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

### Generated Files:
- **Source Scan Details CSV**: Detailed analysis of source scans
- **Target Scan Details CSV**: Detailed analysis of target scans (if provided)
- **Analysis CSV with Comments**: Combined analysis with highlighting comments
- **Highlighted Excel Report**: Color-coded Excel file for easy review
- **Scan Mapping CSV**: Mapping between source and target scan IDs

### File Naming Convention:
- `source_scandetails_YYYYMMDD_HHMMSS.csv`
- `target_scandetails_YYYYMMDD_HHMMSS.csv`
- `analysis_with_comments_YYYYMMDD_HHMMSS.csv`
- `analysis_with_comments_YYYYMMDD_HHMMSS_highlighted.xlsx`

## Configuration

### Database Configuration
The SDK will prompt you for:
- Source Database Instance
- Source Database Password
- Source Username
- Source Password
- Target Database Instance
- Target Database Password
- Target Username
- Target Password

### Security Notes
- All passwords are masked during input
- Credentials are not stored permanently
- Configuration is collected interactively for security

## Dependencies

### Required Python Packages:
- `pandas>=1.5.0` - Data manipulation and analysis
- `numpy>=1.21.0` - Numerical computing
- `psycopg[binary]>=3.0.0` - PostgreSQL database adapter
- `requests>=2.28.0` - HTTP library for API calls
- `openpyxl>=3.0.0` - Excel file generation

### Installation:
```bash
pip install -r requirements.txt
```

## Troubleshooting

### Common Issues:
1. **Database Connection Errors**: Verify credentials and network access
2. **Permission Errors**: Ensure you have write access to the results directory
3. **Missing Dependencies**: Run `pip install -r requirements.txt`
4. **File Not Found**: Check that all required files are in the correct directory

### Error Messages:
- **"Password authentication failed"**: Check database credentials
- **"Permission denied creating folder"**: Check directory permissions
- **"No scan mapping CSV file found"**: Ensure copy script completed successfully

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is part of the retech-us organization and follows the organization's licensing terms.

## Support

For support and questions:
- Create an issue in this repository
- Contact the development team
- Check the troubleshooting section above

---

**Note**: This SDK is designed for internal use within the retech-us organization for scan copying and analysis workflows.