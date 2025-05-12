# Changelog

All notable changes to the Bitget History Downloader project will be documented in this file.

## [Unreleased]

## [0.2.0] - 2025-05-10

### Changed
- **check_proxies.sh**:
  - Removed support for HTTP proxies; now only SOCKS4/SOCKS5 are supported.
  - Changed output file from `proxies_working.txt` to `proxies.txt`.
  - Simplified proxy checking logic by removing retry attempts and port filtering.
  - Added debug mode (`--debug`) for detailed logging.
- **download_history.sh**:
  - Removed retry logic for failed downloads (`MAX_RETRIES`).
  - Simplified proxy rotation to occur only based on download frequency (`ROTATION_FACTOR`), not on errors.
  - Adjusted timeouts for `trades` downloads: `--connect-timeout 20 --max-time 60` (was 5/30).
  - Changed `check_file_type` to log and return 1 for invalid files instead of terminating with `err`.
  - Updated `import_to_db` to skip invalid CSV files with logging instead of terminating.
  - Added debug mode (`--debug`) for detailed logging.
- **General**:
  - Improved temporary file cleanup in `/tmp/`.
  - Simplified error handling to skip invalid files without stopping the script.
  - Updated `README.md` and `README-ru.md` to reflect new features and usage.

### Fixed
- **download_history.sh**:
  - Fixed premature termination after successful download due to incorrect `break` in `depth` processing.