# schwab-websocket-stream-capture

Stream and capture Charles Schwab WebSocket market data (equities + options) to local Parquet.

## Features

- **File-based access token**: Trader API token (Account Access + User Preferences) from `schwab_access_token.txt` — no OAuth in this client
- **WebSocket streaming**: Schwab Streaming API login + subscriptions on one connection
- **Chart data**: `CHART_EQUITY` from `symbols.txt`
- **Options data**: `LEVELONE_OPTIONS` from `options_symbols.txt` or via `--option-chain`
- **Option-chain workflow**: `--option-chain SYMBOL --min-dte N` with optional `--strike-range` (ATM ± N)
- **Parquet storage**: buffered writes to `{data-dir}/equity|options/*.parquet` (zstd)
- **Market hours**: waits for 9:30 AM ET; disconnects at 4:00:30 PM ET
- **Timezone aware**: all session timing uses US/Eastern

## Requirements

- Python 3.8+
- Virtual environment (venv)
- Valid Schwab **Trader API** access token with:
  - **Accounts and Trading → Account Access**
  - **Accounts and Trading → User Preferences**
- A **Market Data-only** token is **not** sufficient (streaming needs `userPreference`)

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

## Configuration

This client does **not** run OAuth. It reads a pre-obtained access token, then either:

1. loads symbols from local files, or
2. fetches a chain with `--option-chain` / `--min-dte` / `--strike-range`

Copy the example templates (committed) into local files (gitignored):

```bash
cp schwab_access_token.txt.example schwab_access_token.txt
cp symbols.txt.example symbols.txt
cp options_symbols.txt.example options_symbols.txt
```

### Files that stay local (gitignored)

| Path | Why ignored |
|------|-------------|
| `schwab_access_token.txt` | Secret access token |
| `symbols.txt`, `options_symbols.txt` | Local symbol lists |
| `data/`, `data_*/`, `memory_samples/`, `spy_0dte/`, `spy_chain/` | Captured market data (`.parquet`) |
| `.venv/`, `__pycache__/` | Environment / bytecode |

Committed templates: `*.example`. Use `--data-dir` for any output folder; keep large captures out of git.

### 1. Access token — `schwab_access_token.txt`

Streaming starts with `GET /trader/v1/userPreference` (streamer URL + client IDs).

| Rule | Detail |
|------|--------|
| Path | `schwab_access_token.txt` (override with `--token`) |
| Format | One line: raw access token (`#` comment lines ignored) |
| Required API | **Trader API — Account Access** and **User Preferences** |
| Do **not** use | Market Data-only token (**Trader API — Market Data**) |
| Do not include | `Bearer ` prefix, quotes, JSON |
| Template | `schwab_access_token.txt.example` |

| Token type | `quotes` / `chains` | `userPreference` (WebSocket) |
|------------|---------------------|------------------------------|
| Account Access + User Preferences | Yes (if also entitled) | **Yes** |
| Market Data only | Yes | **No (401)** |

```text
I0.b2F1dGgyLmNkYy5zY2h3YWIuY29t....@
```

Obtain a Trader API token (e.g. via `schwab-auth-manager`) from an app approved for
**Accounts and Trading**, then paste it into `schwab_access_token.txt`.

```bash
curl -X GET 'https://api.schwabapi.com/trader/v1/userPreference' \
  -H 'accept: application/json' \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

### 2. Equity symbols — `symbols.txt` (optional)

| Rule | Detail |
|------|--------|
| Path | `symbols.txt` (override with `--symbols`) |
| Format | Comma-separated equity tickers |
| Example | `SPY,TSLA,AAPL` |
| Template | `symbols.txt.example` |

Empty/missing → skip `CHART_EQUITY`. Ignored when `--option-chain` is set.

### 3. Option symbols — `options_symbols.txt` (optional)

| Rule | Detail |
|------|--------|
| Path | `options_symbols.txt` (override with `--options`) |
| Format | Comma-separated Schwab OSI option symbols |
| Spacing | Keep Schwab padding (root space-padded to 6 chars) |
| Example | `AAPL  251031C00110000,AAPL  251031C00120000` |
| Template | `options_symbols.txt.example` |

Empty/missing → skip `LEVELONE_OPTIONS` in file mode. For a full expiration,
prefer `--option-chain` instead of hand-building this file.

## Usage

Activate the venv first:

```bash
source .venv/bin/activate
```

### How to run (by mode)

#### 1. Equity only (`CHART_EQUITY`)

Put tickers in `symbols.txt` and leave options empty (or point `--options` at an empty file):

```bash
# symbols.txt → SPY,TSLA,AAPL
# options_symbols.txt → (empty)
python schwab_websocket_client.py

# Or explicit files
echo 'SPY,TSLA,AAPL' > equities_only.txt
echo -n '' > empty_options.txt
python schwab_websocket_client.py --symbols equities_only.txt --options empty_options.txt
```

Output: `{data-dir}/equity/*.parquet`

#### 2. Options only (`LEVELONE_OPTIONS` from a contract list)

Leave equities empty; list OSI contracts in `options_symbols.txt`:

```bash
# symbols.txt → (empty)
# options_symbols.txt → AAPL  251031C00110000,AAPL  251031P00110000
python schwab_websocket_client.py

# Or explicit files
echo -n '' > empty_equities.txt
echo 'AAPL  251031C00110000,AAPL  251031P00110000' > contracts.txt
python schwab_websocket_client.py --symbols empty_equities.txt --options contracts.txt
```

Output: `{data-dir}/options/*.parquet`

#### 3. Mixed (equity + listed option contracts)

Both files have content:

```bash
# symbols.txt → SPY,QQQ
# options_symbols.txt → SPY   260916C00500000,SPY   260916P00500000
python schwab_websocket_client.py --data-dir ./mixed_session
```

Output: `{data-dir}/equity/*.parquet` and `{data-dir}/options/*.parquet`

#### 4. One symbol — entire option chain at DTE ≥ N

Fetches **all strikes** for the earliest expiration with days-to-expiration **≥ `--min-dte`**.
Ignores `symbols.txt` / `options_symbols.txt`.

```bash
# SPY, full strike chain, first expiry with DTE >= 10
python schwab_websocket_client.py --option-chain SPY --min-dte 10

# SPY 0DTE (same-day) full chain
python schwab_websocket_client.py --option-chain SPY --min-dte 0 --data-dir ./spy_0dte

# QQQ full chain, DTE >= 5
python schwab_websocket_client.py --option-chain QQQ --min-dte 5
```

`--strike-range` defaults to `0` (= entire chain).

Output: `{data-dir}/options/*.parquet` (one file per contract)

#### 5. One symbol — limited chain (ATM ± N) at DTE ≥ N

Same as above, but only **ATM center ± `--strike-range`** strikes:

```bash
# SPY, DTE >= 10, ATM +/- 25 strikes (calls + puts)
python schwab_websocket_client.py --option-chain SPY --min-dte 10 --strike-range 25

# SPY 0DTE, ATM +/- 10, calls only
python schwab_websocket_client.py --option-chain SPY --min-dte 0 \
  --strike-range 10 --contract-type CALL --data-dir ./spy_0dte_atm10
```

| Mode | Key flags / files |
|------|-------------------|
| Equity only | `symbols.txt` filled, `options_symbols.txt` empty |
| Options only (list) | `symbols.txt` empty, `options_symbols.txt` filled |
| Mixed | both files filled |
| 1 symbol, **entire** chain @ DTE | `--option-chain SYM --min-dte N` (`--strike-range 0`) |
| 1 symbol, **limited** chain @ DTE | `--option-chain SYM --min-dte N --strike-range K` |

### Option-chain flags (modes 4–5)

Fetches contracts for the earliest expiration with **DTE ≥ `--min-dte`**,
subscribes on one connection, writes **`.parquet`** until market close.

| Flag | Meaning |
|------|---------|
| `--option-chain SYMBOL` | Enable chain workflow for this underlying |
| `--min-dte N` | Earliest expiration with DTE **≥ N** (default `0`) |
| `--strike-range N` | `0` = entire chain (default); `N > 0` = ATM ± N |
| `--contract-type` | `ALL` (default), `CALL`, or `PUT` |
| `--batch-size` | Contracts per SUBS/ADD batch (default `50`) |
| `--parquet-flush-rows` | Buffer size per symbol before disk flush (default `250`) |
| `--token` / `-t` | Access token file |
| `--data-dir` / `-d` | Parquet output root (default `data`) |
| `--debug` | Verbose streamer logging |

In chain mode, `--symbols` / `--options` files are ignored.

**Capacity:** verified **310** SPY contracts (full expiration) on one WebSocket
without `REACHED_SYMBOL_LIMIT` (code 19).

### Storage (Parquet / zstd)

Equity and options are written as **columnar Parquet with zstd** (typically smaller
and faster to scan than CSV.gz for the same wide quote rows).

Earlier CSV.gz estimate for a ~310-contract SPY chain was **~1–2 MiB/min**
(~390–780 MB/day RTH, ~960 MB–1.92 GB/day ETH). Parquet is expected to be
**similar or smaller**; re-measure on your first live session if you need exact sizing.

Rows are buffered in memory and flushed every `--parquet-flush-rows` (and on a
1s timer / disconnect) so the stream does not open a new file per tick.

### All CLI options

```bash
python schwab_websocket_client.py [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--symbols`, `-s` | Equity symbols file (default `symbols.txt`) |
| `--options`, `-o` | Option symbols file (default `options_symbols.txt`) |
| `--option-chain` | Underlying ticker for chain-capture mode |
| `--min-dte` | Min DTE for chain mode (default `0`) |
| `--strike-range` | `0` = full chain; `N` = ATM ± N (default `0`) |
| `--contract-type` | `ALL` \| `CALL` \| `PUT` (default `ALL`) |
| `--batch-size` | Chain SUBS/ADD batch size (default `50`) |
| `--parquet-flush-rows` | Rows buffered per symbol before flush (default `250`) |
| `--token`, `-t` | Access token file (default `schwab_access_token.txt`) |
| `--data-dir`, `-d` | Output directory for `.parquet` files (default `data`) |
| `--debug` | Verbose logging |

### What happens

**File mode** (`--symbols` / `--options`):

1. Read Trader API access token  
2. `GET /trader/v1/userPreference` → streamer URL / client IDs  
3. Load equity and/or option symbols from files  
4. Wait for market open if needed  
5. Connect, log in, subscribe to `CHART_EQUITY` and/or `LEVELONE_OPTIONS`  
6. Buffer and flush **`.parquet`** under `{data-dir}/equity/` and `{data-dir}/options/`  
7. Disconnect at 4:00:30 PM ET (flushes remaining buffers)  

**Chain mode** (`--option-chain SYMBOL --min-dte N`):

1. Read token + streamer preferences  
2. Resolve earliest expiration with DTE ≥ N  
3. Fetch contracts (`ALL` / `CALL` / `PUT`), full chain or ATM ± `--strike-range`  
4. Wait for market open if needed; connect and log in  
5. Batched `SUBS` / `ADD` on `LEVELONE_OPTIONS`  
6. Stream into `{data-dir}/options/*.parquet` until market close  

## Data format

Default root: `data/` (override with `--data-dir`). All exports are **Parquet (zstd)**:

- Equity: `{data-dir}/equity/SYMBOL.parquet`
- Options: `{data-dir}/options/SYMBOL.parquet` (spaces stripped from filenames)

```python
import pandas as pd
df = pd.read_parquet("data/options/SPY260928C00750000.parquet")
```

### Chart data (`CHART_EQUITY`)

| Column | Meaning |
|--------|---------|
| `key` | Symbol |
| `sequence` | Candle sequence |
| `open`, `high`, `low`, `close` | OHLC |
| `volume` | Volume |
| `time` | Epoch ms (API) |
| `chart_day` | Chart day |
| `time_et` | ET datetime string (added) |

### Options data (`LEVELONE_OPTIONS`)

All mapped level-one fields (bid/ask/last, size, greeks, IV, exchange, etc.) plus:

| Column | Meaning |
|--------|---------|
| `quote_time`, `trade_time`, `indicative_quote_time` | Epoch ms |
| `time_et` | ET datetime from best available timestamp (added) |

Partial option updates are merged with the previous values for that contract before each buffered write.

## Dependencies

- `httpx` — REST (user preferences, expiration chain, option chains)
- `websocket-client` — streaming socket
- `pytz` — US/Eastern session times
- `pandas` — row batches → tables
- `pyarrow` — Parquet writers (zstd)

## Notes

- Token must include Trader API **Account Access** + **User Preferences**
- **310** simultaneous `LEVELONE_OPTIONS` subscriptions verified on one connection
- Equity and options both export as **`.parquet`** (zstd); typically ≤ prior CSV.gz footprint for the same chain
- Session window enforced: 9:30 AM – 4:00:30 PM ET; weekends exit early
- Keep tokens and captured `.parquet` files out of git (see `.gitignore`)
