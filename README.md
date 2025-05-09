# Bitget History Downloader

A Bash script to download and process historical market data (trades and depth) from Bitget's public API for cryptocurrency pairs like BTCUSDT. The script mirrors data locally, converts it to CSV, and imports it into an SQLite database with deduplication.

## Features

- **Data Types**: Supports `trades` (trade history) and `depth` (order book snapshots).
- **Markets**: Handles both spot and futures markets.
- **Local Mirroring**: Stores `.zip` files in `data/offline/` to avoid redundant downloads.
- **Efficient Processing**: Converts `.xlsx` (depth) to `.csv`, skips existing files, and uses temporary folders for extraction.
- **Deduplication**: Imports data into SQLite with checks to prevent duplicate entries (`trade_id` for trades, `timestamp` for depth).
- **Proxy Support**: Uses SOCKS4/SOCKS5 proxies with fallback to a default proxy.

## Prerequisites

- **Linux/Unix** environment (e.g., Ubuntu, macOS).
- **Dependencies**:
  - `curl`: For downloading files.
  - `unzip`: For extracting `.zip` files.
  - `sqlite3`: For database operations.
  - `ssconvert` (from `gnumeric`): For converting `.xlsx` to `.csv`.
- Install on Ubuntu/Debian:
  ```bash
  sudo apt-get install curl unzip sqlite3 gnumeric
  ```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/magf/bitget-history.git
   cd bitget-history
   ```

2. Make the script executable:
   ```bash
   chmod +x download_history.sh
   ```

## Usage

Run the script with parameters to specify the trading pair, data type, market, and date range:

```bash
./download_history.sh --pair BTCUSDT --type depth --market spot --start-date 2024-05-01 --end-date 2024-05-02
```

### Parameters

- `--pair`: Trading pair (e.g., `BTCUSDT`). Default: `BTCUSDT`.
- `--type`: Data type (`trades` or `depth`). Required.
- `--market`: Market type (`spot` or `futures`). Default: `spot`.
- `--start-date`: Start date (YYYY-MM-DD). Default: 1 year ago.
- `--end-date`: End date (YYYY-MM-DD). Default: today.

### Output

- **ZIP files**: Stored in `data/offline/` (e.g., `data/offline/depth/BTCUSDT/1/20250501.zip`).
- **CSV files**: Stored in `data/csv/` (e.g., `data/csv/depth/BTCUSDT/20250501.csv`).
- **Database**: SQLite database in `data/history_<PAIR>_<MARKET>.db` (e.g., `data/history_BTCUSDT_spot.db`).

### Example

Download depth data for BTCUSDT (spot) from May 1 to May 2, 2024:

```bash
./download_history.sh --pair BTCUSDT --type depth --market spot --start-date 2024-05-01 --end-date 2024-05-02
```

Check the database:

```bash
sqlite3 data/history_BTCUSDT_spot.db "SELECT COUNT(*) FROM depth;"
```

## Database Schema

- **trades** table:
  - `trade_id` (TEXT, PRIMARY KEY): Unique trade identifier.
  - `timestamp` (INTEGER): Unix timestamp in milliseconds.
  - `price` (REAL): Trade price.
  - `side` (TEXT): Buy or sell.
  - `volume_quote` (REAL): Volume in quote currency.
  - `size_base` (REAL): Volume in base currency.
  - Index: `idx_trades_timestamp` on `timestamp`.

- **depth** table:
  - `timestamp` (INTEGER, PRIMARY KEY): Unix timestamp in milliseconds.
  - `ask_price` (REAL): Best ask price.
  - `bid_price` (REAL): Best bid price.
  - `ask_volume` (REAL): Ask volume.
  - `bid_volume` (REAL): Bid volume.
  - Index: `idx_depth_timestamp` on `timestamp`.

## Notes

- The script checks for existing `.zip` and `.csv` files to avoid redundant downloads and conversions.
- For `depth`, files are identical for spot (`1`) and futures (`2`), so the script reuses them if available.
- Temporary files are stored in `/tmp/` and cleaned up after processing.
- Proxies are sourced from a public list with a fallback to a default proxy (`socks5://178.217.101.5:1080`).

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contributing

Feel free to open issues or submit pull requests on GitHub.

## Author

- Maxim Gajdaj <maxim.gajdaj@gmail.com>
