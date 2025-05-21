# Coinbase Candle Collector and Aggregator by Jules AI

## Project Overview

This project is designed to collect, store, and aggregate cryptocurrency candlestick data from the Coinbase exchange. It captures real-time trade information to build 1-minute candles and then aggregates them into various larger timeframes (e.g., 5-minute, 15-minute, 1-hour, 4-hour, 1-day, 1-week). This allows for detailed market analysis across different resolutions.

## Core Components

*   **`dbUtils.js`**: 
    *   Manages SQLite database connections (`candles.db` by default).
    *   Creates tables for different candle intervals (e.g., `candles_1`, `candles_3`, ..., `candles_10080`).
    *   Standard intervals handled: 1m, 3m, 5m, 10m, 15m, 30m, 1h, 4h, 1d, 1w.

*   **`candle1mCollector.js`**:
    *   Connects to Coinbase Pro WebSocket API for real-time trade data (`matches`).
    *   Fetches a list of tradable products (e.g., BTC-USD) via REST API.
    *   Constructs 1-minute candlestick data (Open, High, Low, Close, Volume) from live trades.
    *   Saves these 1-minute candles into the `candles_1` database table.
    *   Includes error handling and WebSocket reconnection logic.

*   **`candleAggregator.js`**:
    *   Periodically reads data from finer-grained candle tables (e.g., `candles_1`).
    *   Aggregates this data to build coarser-grained candles (e.g., `candles_5`, `candles_15`, up to `candles_10080`).
    *   Calculates Open, High, Low, Close, and sums Volume for the aggregated periods.
    *   Aligns candle start times correctly for various intervals, including special handling for daily, weekly, and multi-hour candles based on UTC.
    *   Implements a cleanup mechanism to remove old candles from the database, keeping a defined maximum number of recent candles per interval.

*   **`candleHistoryCollector.js` / `candleHistoryCollectorV2.js`**:
    *   (Assumed) These scripts are likely responsible for fetching historical candlestick data from Coinbase. This is useful for backfilling the database or ensuring data completeness. *The exact mechanism and differences between V1 and V2 would require further inspection of these files.*

*   **`candleIntegrityChecker.js`**:
    *   (Assumed) This script likely provides utilities to validate the integrity and continuity of the collected candlestick data in the database.

*   **`server.js`**:
    *   (Assumed based on `package.json` and `express` dependency) This is the main entry point of the application, likely setting up an Express.js web server. It might serve API endpoints to access the candle data or manage the collector/aggregator processes. The `npm start` command executes this file.

*   **Other scripts**:
    *   Files like `pyvenv.cfg` suggest a Python virtual environment might be used for some auxiliary tasks or development, but the core project is Node.js.
    *   `.gitignore` specifies intentionally untracked files by Git.
    *   `package.json` and `package-lock.json` manage Node.js project metadata, dependencies, and script commands.

## Data Flow

1.  **Real-time Data Collection**:
    *   `candle1mCollector.js` connects to the Coinbase WebSocket feed.
    *   Live trades (`matches`) for subscribed currency pairs (e.g., BTC-USD) are received.
    *   These trades are processed in real-time to construct 1-minute candles.
    *   The newly formed 1-minute candles (OHLCV data) are saved into the `candles_1` table in the SQLite database.

2.  **Data Aggregation**:
    *   `candleAggregator.js` runs periodically.
    *   It reads data from the `candles_1` table (and subsequently from other aggregated tables like `candles_5`, `candles_30`, etc.).
    *   It groups these smaller interval candles into larger timeframes (e.g., 1-minute candles are grouped to form 3-minute and 5-minute candles; 5-minute candles are grouped to form 10-minute and 15-minute candles, and so on).
    *   The aggregated OHLCV data for these new timeframes is then saved into their respective tables (e.g., `candles_3`, `candles_5`, `candles_10`, `candles_60`, `candles_1440`).

3.  **Data Storage**:
    *   All candlestick data, both 1-minute and aggregated, is stored in an SQLite database (`candles.db`).
    *   Each timeframe has its own table (e.g., `candles_1` for 1-minute, `candles_60` for 1-hour).

4.  **Historical Data (Assumed)**:
    *   `candleHistoryCollector.js` (and its V2) would fetch historical data, likely from a Coinbase API, and populate the relevant candle tables directly. This data then becomes available for viewing or further aggregation if needed.

5.  **Data Access (Assumed)**:
    *   The `server.js` (Express application) would potentially provide an API for external applications or a frontend to query the candlestick data from the database.

## Database Schema

The project uses an SQLite database (default filename: `candles.db`) to store candlestick data. Multiple tables are created, one for each supported candle interval.

**Table Naming Convention:**

*   `candles_X` where `X` is the interval in minutes (e.g., `candles_1` for 1-minute candles, `candles_60` for 60-minute candles, `candles_1440` for 1-day candles).

**Common Table Structure:**

All `candles_X` tables share the following columns:

*   `code` (TEXT): The trading pair symbol (e.g., 'BTC-USD').
*   `tms` (INTEGER): The Unix timestamp (in seconds) representing the start time of the candle (UTC).
*   `op` (REAL): The opening price of the candle.
*   `hp` (REAL): The highest price during the candle's interval.
*   `lp` (REAL): The lowest price during the candle's interval.
*   `cp` (REAL): The closing price of the candle.
*   `tv` (REAL): The trading volume during the candle's interval.

**Primary Key:**

*   Each table has a composite primary key: `(code, tms)`. This ensures that for any given trading pair, there's only one candle entry for a specific start time within that interval table.

**Intervals and Corresponding Tables:**

The `dbUtils.js` script sets up tables for the following intervals by default:
*   1 minute (`candles_1`)
*   3 minutes (`candles_3`)
*   5 minutes (`candles_5`)
*   10 minutes (`candles_10`)
*   15 minutes (`candles_15`)
*   30 minutes (`candles_30`)
*   60 minutes (`candles_60`) - 1 hour
*   240 minutes (`candles_240`) - 4 hours
*   1440 minutes (`candles_1440`) - 1 day
*   10080 minutes (`candles_10080`) - 1 week

## Setup and Running

**1. Prerequisites:**

*   Node.js (which includes npm) must be installed on your system.
*   Git (for cloning the repository, if applicable).

**2. Installation:**

*   Clone the repository (if you haven't already):
    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```
*   Install the project dependencies using npm:
    ```bash
    npm install
    ```

**3. Running the Application:**

The primary way to run the application (assuming `server.js` orchestrates or provides control for the data collectors and aggregators) is using the `start` script defined in `package.json`:

```bash
npm start
```

This command will typically execute `node server.js`.

**Running Individual Scripts (Alternative):**

If you need to run specific components like the 1-minute collector or the aggregator independently (e.g., for testing or if `server.js` does not automatically manage them), you can execute them directly with Node.js:

```bash
node candle1mCollector.js
node candleAggregator.js
# and similarly for other scripts like candleHistoryCollector.js
```

**Configuration:**

*   **Database:** The database filename defaults to `candles.db` in the root directory. This can be changed in `dbUtils.js` if needed.
*   **Coinbase Products:** `candle1mCollector.js` fetches all USD-quoted products from Coinbase. This can be modified within the script if you want to collect data for specific pairs only.
*   **Intervals:** Candle intervals for aggregation and table creation are defined in `dbUtils.js` and `candleAggregator.js`.

**Note on Python Virtual Environment:**
The presence of `pyvenv.cfg` suggests a Python virtual environment. If there are auxiliary Python scripts or tools used in conjunction with this project, you might need to set up the Python environment separately. However, the core data collection and aggregation logic is handled by Node.js.
