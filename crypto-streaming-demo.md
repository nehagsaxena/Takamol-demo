# Real-Time Cryptocurrency Streaming Demo

This document describes how to use the cryptocurrency streaming notebooks to demonstrate real-time data processing in Databricks using live market data from Binance.

## Overview

The demo consists of two Databricks notebooks that work together to showcase streaming data pipelines:

1. **Producer Notebook** (`crypto_data_producer.py`) - Connects to Binance WebSocket and writes trade data to Unity Catalog
2. **Analytics Notebook** (`crypto_streaming_analytics.py`) - Uses Auto Loader to process the data with streaming analytics

## Architecture

```
┌────────────────────────┐     ┌──────────────────────────┐     ┌─────────────────────────┐
│   BINANCE WEBSOCKET    │     │   PRODUCER NOTEBOOK      │     │   ANALYTICS NOTEBOOK    │
│   (Real-time trades)   │────▶│   crypto_data_producer   │────▶│   crypto_streaming_     │
│                        │     │                          │     │   analytics             │
│   - BTC/USDT           │     │   - WebSocket client     │     │   - Auto Loader         │
│   - ETH/USDT           │     │   - Batch collection     │     │   - VWAP calculation    │
│   - BNB/USDT           │     │   - JSON file writer     │     │   - Price alerts        │
│   - SOL/USDT           │     │                          │     │   - Delta Lake output   │
│   - XRP/USDT           │     │                          │     │                         │
└────────────────────────┘     └──────────────────────────┘     └─────────────────────────┘
                                         │                               │
                                         ▼                               ▼
                              ┌──────────────────────┐        ┌──────────────────────┐
                              │  Unity Catalog       │        │  Delta Lake Tables   │
                              │  Volume (Landing)    │        │                      │
                              │                      │        │  - trades_raw        │
                              │  /Volumes/takamol_   │        │  - trades_analytics  │
                              │  demo/crypto_        │        │  - price_alerts      │
                              │  streaming/crypto_   │        │                      │
                              │  landing/trades/     │        │                      │
                              └──────────────────────┘        └──────────────────────┘
```

## Data Source: Binance WebSocket API

### Why Binance?

- **Free**: No API key required for public market data streams
- **Reliable**: 99.9%+ uptime, well-documented API
- **High Volume**: 50-200+ trades per second across major pairs
- **Real Data**: Actual cryptocurrency trades happening globally

### WebSocket Endpoint

```
wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/...
```

### Trade Event Schema

Each trade event from Binance contains:

| Field | Type | Description |
|-------|------|-------------|
| `e` | string | Event type ("trade") |
| `E` | long | Event time (Unix ms) |
| `s` | string | Symbol (e.g., "BTCUSDT") |
| `t` | long | Trade ID |
| `p` | string | Price |
| `q` | string | Quantity |
| `b` | long | Buyer order ID |
| `a` | long | Seller order ID |
| `T` | long | Trade time (Unix ms) |
| `m` | boolean | Is buyer the market maker |

## Notebooks

### 1. Producer Notebook: `crypto_data_producer.py`

**Location**: `databricks/notebooks/crypto_data_producer.py`

**Purpose**: Connects to Binance WebSocket and writes trade data as JSON files to Unity Catalog Volume.

#### Configuration

```python
# Trading pairs to monitor
TRADING_PAIRS = ["btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt"]

# How often to write files (seconds)
BATCH_INTERVAL_SECONDS = 5

# How long to run (minutes, or None for continuous)
RUN_DURATION_MINUTES = 3
```

#### Output Format

The producer writes newline-delimited JSON (NDJSON) files:

```
/Volumes/takamol_demo/crypto_streaming/crypto_landing/trades/
├── trades_20240115_143022_abc123.json
├── trades_20240115_143027_def456.json
├── trades_20240115_143032_ghi789.json
└── ...
```

Each file contains enriched trade records:

```json
{"event_type":"trade","event_time":1705329022136,"symbol":"BTCUSDT","trade_id":12345,"price":45000.50,"quantity":0.001,"trade_value_usdt":45.00,"ingestion_time":1705329022140}
```

#### Running Modes

| Mode | Configuration | Use Case |
|------|---------------|----------|
| Demo | `RUN_DURATION_MINUTES = 3` | Quick demo, collect sample data |
| Extended | `RUN_DURATION_MINUTES = 30` | Longer demo with more data |
| Continuous | `RUN_DURATION_MINUTES = None` | Production, run as Databricks Job |

---

### 2. Analytics Notebook: `crypto_streaming_analytics.py`

**Location**: `databricks/notebooks/crypto_streaming_analytics.py`

**Purpose**: Processes trade data using Auto Loader and generates real-time analytics.

#### Auto Loader Configuration

```python
auto_loader_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schema")
    .schema(trade_schema)
    .load(LANDING_PATH)
)
```

#### Output Tables

| Table | Description |
|-------|-------------|
| `trades_raw` | All raw trade records with processing metadata |
| `trades_analytics` | Windowed aggregations (VWAP, volume, price range) |
| `price_alerts` | Detected price movements and volume spikes |

#### Analytics Calculated

| Metric | Description |
|--------|-------------|
| **VWAP** | Volume-Weighted Average Price |
| **Price Range %** | (High - Low) / Avg * 100 |
| **Buy/Sell Ratio** | Buy Volume / Sell Volume |
| **Volume Spike** | Trades > threshold in window |
| **Price Alert** | Price change > threshold % |

## Running the Demo

### Prerequisites

1. Databricks workspace with Unity Catalog enabled
2. Catalog `takamol_demo` exists (or modify configuration)
3. Cluster with internet access to `stream.binance.com`

### Step-by-Step

#### Step 1: Test Connectivity (Optional)

Run the local test script to verify Binance is accessible:

```bash
cd scripts
pip install websocket-client
python test_binance_websocket.py --duration 10
```

#### Step 2: Start the Producer

1. Open `databricks/notebooks/crypto_data_producer.py` in Databricks
2. Attach to a cluster (any size, single node is sufficient)
3. Run all cells
4. Watch trades stream in for the configured duration

#### Step 3: Run Analytics

1. Open `databricks/notebooks/crypto_streaming_analytics.py` in Databricks
2. Attach to a cluster (can be same or different from producer)
3. Run all cells
4. View real-time analytics, VWAP calculations, and alerts

### Demo Narrative

For presentations, follow this flow:

1. **"We're connecting to real cryptocurrency markets"** - Start the producer, show live trades
2. **"Data lands in Unity Catalog"** - Browse the Volume, show JSON files appearing
3. **"Auto Loader picks up new files automatically"** - Start the analytics notebook
4. **"Real-time analytics update as data arrives"** - Show VWAP, volume charts
5. **"Alerts detect market movements"** - Show any price alerts generated
6. **"All data persisted in Delta Lake"** - Query historical data

## Configuration Options

### Producer Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `CATALOG` | `takamol_demo` | Unity Catalog name |
| `SCHEMA` | `crypto_streaming` | Schema name |
| `TRADING_PAIRS` | BTC, ETH, BNB, SOL, XRP | Pairs to monitor |
| `BATCH_INTERVAL_SECONDS` | 5 | File write frequency |
| `RUN_DURATION_MINUTES` | 3 | Demo duration |
| `MAX_TRADES_PER_BATCH` | 1000 | Safety limit per file |

### Analytics Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `PRICE_CHANGE_THRESHOLD_PCT` | 0.5 | Alert on >0.5% price change |
| `VOLUME_SPIKE_THRESHOLD` | 100 | Alert on >100 trades/batch |

## Troubleshooting

### Connection Issues

**Error**: Cannot connect to Binance WebSocket

**Solution**:
- Verify cluster has internet access
- Check if `stream.binance.com` is accessible from your network
- Some corporate networks block WebSocket connections

### No Data in Landing Zone

**Error**: Analytics notebook shows no files

**Solution**:
- Run the producer notebook first
- Verify the `LANDING_PATH` matches between notebooks
- Check Volume permissions in Unity Catalog

### Schema Evolution

**Error**: Schema mismatch errors in Auto Loader

**Solution**:
- Delete the schema checkpoint directory
- Re-run with fresh checkpoint location
- Use `cloudFiles.schemaEvolutionMode = "rescue"` for flexibility

## Production Deployment

For production use:

1. **Run Producer as Job**
   - Set `RUN_DURATION_MINUTES = None`
   - Create Databricks Job with this notebook
   - Configure alerts for job failures

2. **Use Delta Live Tables**
   - Convert analytics notebook to DLT pipeline
   - Get automatic data quality, lineage

3. **Add Monitoring**
   - Track streaming query progress
   - Alert on processing lag
   - Monitor checkpoint sizes

## File Reference

```
04-takamol-demo/
├── databricks/
│   └── notebooks/
│       ├── crypto_data_producer.py      # WebSocket → JSON files
│       └── crypto_streaming_analytics.py # Auto Loader → Delta Lake
├── scripts/
│   └── test_binance_websocket.py        # Local connectivity test
└── docs/
    └── crypto-streaming-demo.md         # This documentation
```

## API Reference

### Binance WebSocket API

- Documentation: https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams
- Endpoint: `wss://stream.binance.com:9443`
- Rate Limits: 5 messages per second per connection
- Max Streams: 1024 streams per connection

### Databricks Auto Loader

- Documentation: https://docs.databricks.com/ingestion/auto-loader/
- Formats: JSON, CSV, Parquet, Avro, ORC, Text, Binary
- Features: Schema inference, evolution, exactly-once processing
