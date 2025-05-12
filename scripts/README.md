# Bitget History Downloader

A Bash script for downloading and processing historical market data (trades and depth) from Bitget's public API for cryptocurrency pairs like BTCUSDT. It mirrors data locally, converts it to CSV, and stores it in an SQLite database with deduplication.

## Features

- Supports `trades` (trade history) and `depth` (order book snapshots) for spot and futures markets.
- Mirrors `.zip` files in `data/offline/` to skip redundant downloads.
- Converts `.xlsx` (depth) to `.csv` and stores them in `data/csv/`.
- Imports data into SQLite with deduplication (`trade_id` for trades, `timestamp` for depth).
- Uses SOCKS4/SOCKS5 proxies with rotation based on download frequency.
- Configurable via `data/config` for sensitive settings like proxies.
- Debug mode (`--debug`) for detailed logging.
- Skips invalid files (non-Zip or non-CSV) with logging instead of failing.

## Prerequisites

- Linux/Unix environment (e.g., Ubuntu, macOS).
- Dependencies:
  - `curl`: For downloading files.
  - `unzip`: For extracting `.zip` files.
  - `sqlite3`: For database operations.
  - `ssconvert` (from `gnumeric`): For converting `.xlsx` to `.csv`.
- Install on Ubuntu/Debian:
  ```bash
  sudo apt-get install curl unzip sqlite3 gnumeric
  ```

## Configuration

Sensitive settings like the proxy can be defined in `data/config` (not included in the repository for security). Create it manually if needed.

Example `data/config`:
```bash
MY_PROXY="socks5://1.2.3.4:1080"
```

If access to `cdn.jsdelivr.net` (used for proxy lists) is restricted, specify a local SOCKS5 proxy in `data/config`. Example:
```bash
MY_PROXY="socks5://1.2.3.4:1080"
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/magf/bitget-history.git
   cd bitget-history
   ```

2. Make scripts executable:
   ```bash
   chmod +x check_proxies.sh download_history.sh
   ```

3. Generate proxy list:
   ```bash
   ./check_proxies.sh --timeout 3
   ```

## Usage

1. **Check proxies**:
   - Generate a list of working SOCKS4/SOCKS5 proxies:
     ```bash
     ./check_proxies.sh --timeout 3 --debug
     ```
   - Parameters:
     - `--timeout`: Connection timeout in seconds (default: 3).
     - `--debug`: Enable detailed logging.

2. **Download history**:
   - Run the script with parameters for pair, data type, market, and date range:
     ```bash
     ./download_history.sh --pair BTCUSDT --type depth --market spot --start-date 2024-05-01 --end-date 2024-05-02 --debug
     ```
   - Parameters:
     - `--pair`: Trading pair (e.g., `BTCUSDT`). Default: `BTCUSDT`.
     - `--type`: Data type (`trades` or `depth`). Required.
     - `--market`: Market type (`spot` or `futures`). Default: `spot`.
     - `--start-date`: Start date (YYYY-MM-DD). Default: 1 year ago.
     - `--end-date`: End date (YYYY-MM-DD). Default: today.
     - `--debug`: Enable detailed logging.

### Output

- **Proxy lists**:
  - Raw proxies: `data/proxies_raw.txt`.
  - Working proxies: `data/proxies.txt`.
- **ZIP files**: Stored in `data/offline/` (e.g., `data/offline/depth/BTCUSDT/1/20240501.zip`).
- **CSV files**: Stored in `data/csv/` (e.g., `data/csv/depth/BTCUSDT/20240501.csv`).
- **Database**: SQLite database in `data/history_<PAIR>_<MARKET>.db` (e.g., `data/history_BTCUSDT_spot.db`).

### Example

Download depth data for BTCUSDT (spot) from May 1 to May 2, 2024:

```bash
./check_proxies.sh --timeout 3
./download_history.sh --pair BTCUSDT --type depth --market spot --start-date 2024-05-01 --end-date 2024-05-02 --debug
```

Check the database:
```bash
sqlite3 data/history_BTCUSDT_spot.db "SELECT COUNT(*) FROM depth;"
```

## Database Schema

- **trades** table:
  - `trade_id` (TEXT, PRIMARY KEY): Unique trade ID.
  - `timestamp` (INTEGER): Unix timestamp (ms).
  - `price` (REAL): Trade price.
  - `side` (TEXT): Buy or sell.
  - `volume_quote` (REAL): Volume in quote currency.
  - `size_base` (REAL): Volume in base currency.
  - Index: `idx_trades_timestamp` on `timestamp`.

- **depth** table:
  - `timestamp` (INTEGER, PRIMARY KEY): Unix timestamp (ms).
  - `ask_price` (REAL): Best ask price.
  - `bid_price` (REAL): Best bid price.
  - `ask_volume` (REAL): Ask volume.
  - `bid_volume` (REAL): Bid volume.
  - Index: `idx_depth_timestamp` on `timestamp`.

## Notes

- Skips existing `.zip` and `.csv` files to avoid redundant downloads/conversions.
- Depth files are identical for spot (`1`) and futures (`2`), reused if available.
- Temporary files are stored in `/tmp/` and cleaned up after processing.
- All data files (`data/`) are excluded from the repository via `.gitignore`.
- Proxy rotation occurs based on the number of downloads, not errors, to simplify error handling.
- Invalid files (non-Zip or non-CSV) are logged and skipped without stopping the script.

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## Contributing

Open issues or submit pull requests on GitHub.

## Author

Maxim Gajdaj <maxim.gajdaj@gmail.com>