"""
schwab-websocket-stream-capture

Schwab WebSocket stream capture (no OAuth in this client):
1. Read Schwab access token from a local file (Trader API: Account Access + User Preferences)
2. Load equity/option symbols from files, OR fetch a full option chain via --option-chain
3. Connect to the Schwab streaming WebSocket
4. Subscribe to CHART_EQUITY and/or LEVELONE_OPTIONS
5. Persist streamed data to Parquet (`.parquet`, zstd)

Required file formats
---------------------
Access token file (default: schwab_access_token.txt):
  - Single line containing the raw access token only
  - Must be a Trader API token with Account Access + User Preferences
    (not a Market Data-only token — streaming needs GET /trader/v1/userPreference)
  - No "Bearer " prefix, no quotes, no JSON; # comment lines are allowed
  - Example: see schwab_access_token.txt.example

Equity symbols file (default: symbols.txt):
  - One line of comma-separated ticker symbols
  - Example: SPY,TSLA,AAPL
  - See symbols.txt.example

Option symbols file (default: options_symbols.txt):
  - One line of comma-separated Schwab option contract symbols (OSI style)
  - Preserve spacing as returned by Schwab (root is space-padded to 6 chars)
  - Example: AAPL  251031C00110000,AAPL  251031C00120000
  - See options_symbols.txt.example

Option-chain CLI mode (--option-chain SYMBOL --min-dte N):
  - Fetches call+put contracts for the earliest expiration with DTE >= N
  - Default: entire strike chain; optional --strike-range K = ATM center +/- K strikes
  - Subscribes contracts on one LEVELONE_OPTIONS stream (tested up to 310)
  - Writes buffered Parquet (`.parquet`, zstd) under data/options/

Either symbols file may be empty or omitted; only non-empty lists are subscribed.
"""
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple

import json
import time
import threading
import os
import argparse

import httpx
import websocket
import pytz
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# Default configuration file paths and formats
DEFAULT_ACCESS_TOKEN_FILE = 'schwab_access_token.txt'
DEFAULT_EQUITY_SYMBOLS_FILE = 'symbols.txt'
DEFAULT_OPTION_SYMBOLS_FILE = 'options_symbols.txt'
DEFAULT_DATA_OUTPUT_DIR = 'data'
MARKET_DATA_BASE = 'https://api.schwabapi.com/marketdata/v1'
DEFAULT_PARQUET_FLUSH_ROWS = 250
DEFAULT_PARQUET_COMPRESSION = 'zstd'

# Schwab streamer response codes relevant to subscription limits
STREAM_CODE_SUCCESS = 0
STREAM_CODE_REACHED_SYMBOL_LIMIT = 19


class SchwabWebSocketClient:
    """Schwab WebSocket client for streaming chart and options data"""

    def __init__(self, debug: bool = False,
                 symbols_filepath: str = DEFAULT_EQUITY_SYMBOLS_FILE,
                 option_symbols_filepath: str = DEFAULT_OPTION_SYMBOLS_FILE,
                 access_token_filepath: str = DEFAULT_ACCESS_TOKEN_FILE,
                 data_output_dir: str = DEFAULT_DATA_OUTPUT_DIR,
                 equity_symbols: Optional[List[str]] = None,
                 option_symbols: Optional[List[str]] = None,
                 auto_subscribe: bool = True,
                 parquet_flush_rows: int = DEFAULT_PARQUET_FLUSH_ROWS,
                 parquet_compression: str = DEFAULT_PARQUET_COMPRESSION):
        self.debug = debug
        self.access_token_filepath = access_token_filepath
        self.data_output_dir = data_output_dir
        self.auto_subscribe = auto_subscribe
        self.parquet_flush_rows = max(1, parquet_flush_rows)
        self.parquet_compression = parquet_compression

        # Prefer in-memory symbol lists when provided (e.g. --option-chain mode)
        if equity_symbols is not None:
            self.symbols = list(equity_symbols)
        else:
            self.symbols = self.load_symbols_from_file(symbols_filepath)

        if option_symbols is not None:
            self.option_symbols = list(option_symbols)
        else:
            self.option_symbols = self.load_symbols_from_file(
                option_symbols_filepath)

        # WebSocket connection
        self.ws: Optional[websocket.WebSocketApp] = None
        self.running = False
        self.connected = False
        self.request_id = 1
        self.subscriptions = {}

        # Subscription command response tracking (SUBS/ADD/UNSUBS)
        self._subscription_lock = threading.Lock()
        self._subscription_events: Dict[int, threading.Event] = {}
        self._subscription_results: Dict[int, Dict[str, Any]] = {}

        # Parquet write buffers / open writers (row-group append)
        self._parquet_lock = threading.Lock()
        self._parquet_buffers: Dict[str, List[Dict[str, Any]]] = {}
        self._parquet_writers: Dict[str, pq.ParquetWriter] = {}

        # Store previous option values for merging partial updates
        self.previous_option_values: Dict[str, Dict] = {}

        # Optional callback: on_option_update(symbol: str, parsed: dict) -> None
        self.on_option_update = None

        # Market hours (ET timezone) - always use ET regardless of local timezone
        self.et_tz = pytz.timezone('US/Eastern')
        # Store as time objects (used for comparison with ET time)
        self.market_open_time = datetime.strptime(
            '09:30:00', '%H:%M:%S').time()
        self.market_close_time = datetime.strptime(
            '16:00:30', '%H:%M:%S').time()

        # Create data directories if they don't exist
        os.makedirs(f'{self.data_output_dir}/equity', exist_ok=True)
        os.makedirs(f'{self.data_output_dir}/options', exist_ok=True)

        # CHART_EQUITY field mappings based on Schwab API
        # Fields 2-6 are OHLCV (Open, High, Low, Close, Volume) and field 7 is timestamp
        self.chart_equity_fields = {
            0: 'key',        # Symbol (ticker symbol)
            1: 'sequence',   # Sequence - Identifies the candle minute
            2: 'open',       # Open Price - Opening price for the minute
            3: 'high',       # High Price - Highest price for the minute
            4: 'low',        # Low Price - Chart's lowest price for the minute
            5: 'close',      # Close Price - Closing price for the minute
            6: 'volume',     # Volume - Total volume for the minute
            7: 'time',       # Chart Time - Milliseconds since Epoch
            8: 'chart_day'   # Chart Day
        }

        # Get user preferences and store streamer info
        self.user_preferences = self.get_user_preferences()
        if not self.user_preferences or 'streamerInfo' not in self.user_preferences:
            raise Exception("Could not get user preferences")

        # Store streamer info
        self.streamer_info = self.user_preferences['streamerInfo'][0]
        self.schwab_websocket_client_customer_id = self.streamer_info.get(
            'schwabClientCustomerId')
        if not self.schwab_websocket_client_customer_id:
            raise Exception("No SchwabClientCustomerId in streamer info")

    def load_symbols_from_file(self, filepath: str) -> List[str]:
        """
        Load symbols from a comma-separated file.

        Format: single line (or multi-line) of comma-separated symbols.
        Equity example: SPY,TSLA,AAPL
        Option example: AAPL  251031C00110000,AAPL  251031C00120000
        Missing/empty files yield an empty list (that subscription is skipped).
        """
        try:
            if not os.path.exists(filepath):
                print(
                    f"⚠️ Symbols file {filepath} not found. Using empty list.")
                return []

            with open(filepath, 'r') as f:
                content = f.read().strip()
                if not content:
                    return []

            # Split by comma; strip surrounding whitespace only (keep option padding)
            symbols = [s.strip() for s in content.replace('\n', ',').split(',') if s.strip()]
            print(
                f"✅ Loaded {len(symbols)} symbols from {filepath}: {', '.join(symbols)}")
            return symbols
        except Exception as e:
            print(f"❌ Error loading symbols from {filepath}: {e}")
            return []

    def get_access_token(self) -> str:
        """
        Read access token from file.

        Required format for {access_token_filepath}:
          - Plain text file with the raw Trader API access token on one line
          - Must authorize Accounts and Trading: Account Access + User Preferences
            (Market Data-only tokens are not enough — userPreference will 401)
          - No "Bearer " prefix, no quotes, no JSON wrapper
          - Lines starting with # and blank lines are ignored
          - Copy from schwab_access_token.txt.example and replace the placeholder
        """
        try:
            if not os.path.exists(self.access_token_filepath):
                raise Exception(
                    f"Access token file not found: {self.access_token_filepath}. "
                    f"Create it from schwab_access_token.txt.example "
                    f"(Trader API Account Access + User Preferences token).")

            with open(self.access_token_filepath, 'r') as f:
                lines = f.readlines()

            token = ""
            for line in lines:
                stripped = line.strip()
                if not stripped or stripped.startswith('#'):
                    continue
                token = stripped
                break

            if not token:
                raise Exception(
                    f"Access token file is empty: {self.access_token_filepath}")

            # Reject common mis-formats early
            if token.startswith('{') or token.startswith('['):
                raise Exception(
                    f"Access token file must contain the raw token string, not JSON: "
                    f"{self.access_token_filepath}")
            if token.lower().startswith('bearer '):
                token = token[7:].strip()
            if (token.startswith('"') and token.endswith('"')) or (
                    token.startswith("'") and token.endswith("'")):
                token = token[1:-1].strip()

            if not token:
                raise Exception(
                    f"Access token file has no usable token: {self.access_token_filepath}")

            return token

        except Exception as e:
            print(
                f"❌ Error reading access token from {self.access_token_filepath}: {e}")
            raise

    def get_user_preferences(self):
        """Fetch streamer info using the access token read from file (no OAuth)."""
        try:
            access_token = self.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")

            headers = {
                'Authorization': f'Bearer {access_token}',
                'Accept': 'application/json'
            }

            url = "https://api.schwabapi.com/trader/v1/userPreference"

            with httpx.Client() as client:
                response = client.get(url, headers=headers)

            if response.status_code != 200:
                raise Exception(
                    f"User preferences request failed: {response.status_code} - {response.text}")

            return response.json()

        except Exception as e:
            if self.debug:
                print(f"❌ Error getting user preferences: {e}")
            raise

    def _market_data_headers(self) -> Dict[str, str]:
        return {
            'Authorization': f'Bearer {self.get_access_token()}',
            'Accept': 'application/json',
        }

    def find_expiration(
            self, underlying: str, min_dte: int = 0) -> Tuple[str, int]:
        """
        Return (expirationDate YYYY-MM-DD, daysToExpiration) for the earliest
        expiration with daysToExpiration >= min_dte.
        """
        url = f"{MARKET_DATA_BASE}/expirationchain"
        with httpx.Client(timeout=30.0) as client:
            response = client.get(
                url, headers=self._market_data_headers(),
                params={"symbol": underlying})

        if response.status_code != 200:
            raise Exception(
                f"Expiration chain failed: {response.status_code} - {response.text}")

        expirations = response.json().get("expirationList", [])
        if not expirations:
            raise Exception(f"No expirations returned for {underlying}")

        eligible = [
            e for e in expirations
            if int(e.get("daysToExpiration", -1)) >= min_dte
        ]
        if eligible:
            chosen = min(
                eligible, key=lambda e: int(e.get("daysToExpiration", 10**9)))
        else:
            chosen = max(
                expirations, key=lambda e: int(e.get("daysToExpiration", -1)))
            print(
                f"⚠️ No expiration with DTE>={min_dte} for {underlying}; "
                f"using farthest available DTE={chosen.get('daysToExpiration')}")

        exp_date = chosen["expirationDate"]
        dte = int(chosen.get("daysToExpiration", -1))
        print(
            f"🎯 Using {underlying} expiration {exp_date} "
            f"(DTE={dte}, min_dte={min_dte})")
        return exp_date, dte

    def fetch_option_chain_contracts(
            self,
            underlying: str,
            expiration_date: str,
            contract_type: str = "ALL",
            strike_range: Optional[int] = None) -> List[str]:
        """
        Fetch Schwab OSI option symbols for one expiration.

        Args:
            strike_range: If None or 0, request the entire strike chain.
                          If > 0, request ATM center +/- strike_range strikes
                          (Schwab chains `strikeCount`).
        """
        params: Dict[str, Any] = {
            "symbol": underlying,
            "contractType": contract_type,
            "includeUnderlyingQuote": "true",
            "strategy": "SINGLE",
            "fromDate": expiration_date,
            "toDate": expiration_date,
        }
        if strike_range is not None and strike_range > 0:
            params["strikeCount"] = str(strike_range)

        url = f"{MARKET_DATA_BASE}/chains"
        with httpx.Client(timeout=60.0) as client:
            response = client.get(
                url, headers=self._market_data_headers(), params=params)

        if response.status_code != 200:
            raise Exception(
                f"Option chain failed: {response.status_code} - {response.text}")

        chain = response.json()
        symbols: List[str] = []
        for map_name in ("callExpDateMap", "putExpDateMap"):
            exp_map = chain.get(map_name) or {}
            for _exp_key, strikes in exp_map.items():
                for _strike, contracts in strikes.items():
                    for contract in contracts:
                        symbol = contract.get("symbol")
                        if symbol:
                            symbols.append(symbol)

        unique = list(dict.fromkeys(symbols))
        underlying_price = chain.get("underlyingPrice")
        if underlying_price is not None:
            print(f"💰 Underlying price: ${float(underlying_price):.2f}")
        range_desc = (
            f"ATM +/- {strike_range} strikes"
            if strike_range and strike_range > 0
            else "entire strike chain"
        )
        print(
            f"📦 Loaded {len(unique)} {underlying} contracts "
            f"for {expiration_date} (type={contract_type}, {range_desc})")
        return unique

    def load_option_chain(
            self,
            underlying: str,
            min_dte: int = 0,
            contract_type: str = "ALL",
            strike_range: Optional[int] = None) -> Tuple[List[str], str, int]:
        """
        Resolve earliest expiration with DTE >= min_dte and load chain symbols.

        strike_range None/0 = entire chain; >0 = ATM center +/- that many strikes.

        Returns (contracts, expiration_date, dte).
        """
        expiration_date, dte = self.find_expiration(underlying, min_dte=min_dte)
        contracts = self.fetch_option_chain_contracts(
            underlying,
            expiration_date,
            contract_type=contract_type,
            strike_range=strike_range,
        )
        if not contracts:
            raise Exception(
                f"No option contracts found for {underlying} {expiration_date}")
        self.option_symbols = list(contracts)
        return contracts, expiration_date, dte

    def subscribe_option_chain(
            self, contracts: List[str], batch_size: int = 50) -> int:
        """
        Subscribe to a full option chain on one connection (SUBS + ADD batches).

        Returns the number of contracts accepted.
        """
        if not contracts:
            raise Exception("No contracts to subscribe")
        if batch_size < 1:
            raise Exception("batch_size must be >= 1")

        accepted: List[str] = []
        print(
            f"📡 Subscribing to {len(contracts)} LEVELONE_OPTIONS contracts "
            f"(batch_size={batch_size})...")

        first = contracts[:batch_size]
        rest = contracts[batch_size:]
        result = self.subscribe_options_data(
            first, wait_for_response=True, timeout=30.0)
        if not result or result.get("code") != STREAM_CODE_SUCCESS:
            raise Exception(f"Initial SUBS failed: {result}")
        accepted.extend(first)
        print(f"✅ SUBS accepted {len(first)} (total={len(accepted)})")

        idx = 0
        while idx < len(rest):
            batch = rest[idx:idx + batch_size]
            idx += batch_size
            result = self.add_options_data(
                batch, wait_for_response=True, timeout=30.0)
            if not result or result.get("code") != STREAM_CODE_SUCCESS:
                if result and result.get("code") == STREAM_CODE_REACHED_SYMBOL_LIMIT:
                    print(
                        f"🚫 REACHED_SYMBOL_LIMIT after {len(accepted)} contracts")
                    break
                raise Exception(f"ADD failed at total={len(accepted)}: {result}")
            accepted.extend(batch)
            print(f"✅ ADD accepted +{len(batch)} (total={len(accepted)})")

        self.option_symbols = list(accepted)
        self.subscriptions["LEVELONE_OPTIONS"] = list(accepted)
        print(
            f"✅ Option chain subscription ready: "
            f"{len(accepted)}/{len(contracts)} contracts")
        return len(accepted)

    def wait_for_market_open(self):
        """
        Check if it's before 9:30:00 AM ET and wait until market opens if needed.
        All time comparisons use ET timezone, regardless of local timezone.
        Returns True if market is open or will be open today, False if it's weekend.
        """
        # Always get current time in ET timezone
        now_et = datetime.now(self.et_tz)
        current_time_et = now_et.time()
        current_date_et = now_et.date()

        # Check if it's weekend (based on ET date)
        if current_date_et.weekday() >= 5:  # Saturday = 5, Sunday = 6
            print(
                f"📅 Weekend detected ({current_date_et.strftime('%A')}) in ET timezone, market is closed")
            return False

        # Check if it's before market hours (all comparisons in ET)
        if current_time_et < self.market_open_time:
            # Calculate time until market open in ET timezone
            # Combine date and time, then localize to ET (handles DST automatically)
            market_open_dt = self.et_tz.localize(
                datetime.combine(current_date_et, self.market_open_time)
            )

            wait_seconds = (market_open_dt - now_et).total_seconds()

            if wait_seconds > 0:
                wait_hours = int(wait_seconds // 3600)
                wait_minutes = int((wait_seconds % 3600) // 60)
                wait_secs = int(wait_seconds % 60)

                print(
                    f"⏰ Before market hours (ET time). Current ET time: {now_et.strftime('%H:%M:%S %Z')}")
                print(
                    f"📈 Market opens at: {market_open_dt.strftime('%H:%M:%S %Z')}")
                print(
                    f"⏳ Waiting {wait_hours:02d}:{wait_minutes:02d}:{wait_secs:02d} until market open...")

                # Wait with progress indicator (recalculate ET time periodically)
                start_time = time.time()
                while time.time() - start_time < wait_seconds:
                    # Recalculate remaining time based on current ET time
                    remaining_now_et = datetime.now(self.et_tz)
                    remaining = (market_open_dt -
                                 remaining_now_et).total_seconds()
                    if remaining <= 0:
                        break

                    remaining_hours = int(remaining // 3600)
                    remaining_minutes = int((remaining % 3600) // 60)
                    remaining_secs = int(remaining % 60)

                    print(
                        f"\r⏳ Waiting: {remaining_hours:02d}:{remaining_minutes:02d}:{remaining_secs:02d} remaining (ET time)...", end='', flush=True)
                    time.sleep(1)

                print(f"\n🎉 Market is now open (ET time)! Starting streaming...")
            else:
                print(
                    f"✅ Market is already open (current ET time: {now_et.strftime('%H:%M:%S %Z')})")
        else:
            print(
                f"✅ Market is already open (current ET time: {now_et.strftime('%H:%M:%S %Z')})")

        return True

    def is_after_market_close(self):
        """
        Check if current time is after 4:00:30 PM ET.
        All time comparisons use ET timezone, regardless of local timezone.
        """
        # Always get current time in ET timezone
        now_et = datetime.now(self.et_tz)
        current_date_et = now_et.date()

        # Create market close datetime for today in ET timezone
        # Localizing handles DST automatically
        market_close_dt = self.et_tz.localize(
            datetime.combine(current_date_et, self.market_close_time)
        )

        # Compare timezone-aware datetimes (both in ET)
        return now_et >= market_close_dt

    def parse_chart_data(self, content_item: dict) -> Dict:
        """Parse CHART_EQUITY message content using field mappings"""
        try:
            parsed = {}

            for field_key, field_value in content_item.items():
                if field_key.isdigit():
                    field_index = int(field_key)
                    if field_index in self.chart_equity_fields:
                        field_name = self.chart_equity_fields[field_index]

                        # Convert to appropriate data type based on field
                        if field_name in ['open', 'high', 'low', 'close', 'volume']:
                            parsed[field_name] = float(field_value)
                        elif field_name in ['time', 'chart_day', 'sequence']:
                            parsed[field_name] = int(field_value)
                        else:
                            parsed[field_name] = field_value
                elif field_key in ['key', 'seq']:
                    if field_key == 'seq':
                        parsed['sequence'] = int(field_value)
                    else:
                        parsed[field_key] = field_value

            return parsed

        except Exception as e:
            if self.debug:
                print(f"❌ Error parsing CHART_EQUITY data: {e}")
            return {}

    def parse_option_data(self, content_item: dict) -> Dict:
        """Parse LEVELONE_OPTIONS message content, merging with previous values"""
        try:
            symbol = content_item.get('key', '')
            if not symbol:
                if self.debug:
                    print(
                        f"⚠️ Warning: Empty symbol in option data: {content_item}")
                return {}

            # Get previous values for this symbol, or create default structure
            previous_values = self.previous_option_values.get(symbol, {
                'symbol': '',              # 0: Symbol
                'description': '',         # 1: Description
                'bid_price': 0,           # 2: Bid Price
                'ask_price': 0,           # 3: Ask Price
                'last_price': 0,          # 4: Last Price
                'high_price': 0,          # 5: High Price
                'low_price': 0,           # 6: Low Price
                'close_price': 0,         # 7: Close Price
                'total_volume': 0,         # 8: Total Volume
                'open_interest': 0,       # 9: Open Interest
                'volatility': 0,          # 10: Volatility
                'money_intrinsic_value': 0,  # 11: Money Intrinsic Value
                'expiration_year': 0,     # 12: Expiration Year
                'multiplier': 0,          # 13: Multiplier
                'digits': 0,              # 14: Digits
                'open_price': 0,          # 15: Open Price
                'bid_size': 0,            # 16: Bid Size
                'ask_size': 0,            # 17: Ask Size
                'last_size': 0,           # 18: Last Size
                'net_change': 0,          # 19: Net Change
                'strike_price': 0,        # 20: Strike Price
                'contract_type': '',      # 21: Contract Type
                'underlying': '',         # 22: Underlying
                'expiration_month': 0,    # 23: Expiration Month
                'deliverables': '',       # 24: Deliverables
                'time_value': 0,          # 25: Time Value
                'expiration_day': 0,      # 26: Expiration Day
                'days_to_expiration': 0,  # 27: Days to Expiration
                'delta': 0,               # 28: Delta
                'gamma': 0,               # 29: Gamma
                'theta': 0,               # 30: Theta
                'vega': 0,                # 31: Vega
                'rho': 0,                 # 32: Rho
                'security_status': '',    # 33: Security Status
                'theoretical_option_value': 0,  # 34: Theoretical Option Value
                'underlying_price': 0,    # 35: Underlying Price
                'uv_expiration_type': '',  # 36: UV Expiration Type
                'mark_price': 0,          # 37: Mark Price
                # 38: Quote Time (milliseconds since Epoch)
                'quote_time': 0,
                # 39: Trade Time (milliseconds since Epoch)
                'trade_time': 0,
                'exchange': '',          # 40: Exchange
                'exchange_name': '',      # 41: Exchange Name
                'last_trading_day': 0,    # 42: Last Trading Day
                'settlement_type': '',    # 43: Settlement Type
                'net_percent_change': 0,  # 44: Net Percent Change
                'mark_price_net_change': 0,  # 45: Mark Price Net Change
                'mark_price_percent_change': 0,  # 46: Mark Price Percent Change
                'implied_yield': 0,       # 47: Implied Yield
                'is_penny_pilot': False,  # 48: isPennyPilot
                'option_root': '',        # 49: Option Root
                'week_52_high': 0,        # 50: 52 Week High
                'week_52_low': 0,         # 51: 52 Week Low
                'indicative_ask_price': 0,  # 52: Indicative Ask Price
                'indicative_bid_price': 0,  # 53: Indicative Bid Price
                'indicative_quote_time': 0,  # 54: Indicative Quote Time
                'exercise_type': ''       # 55: Exercise Type
            })

            # Create new option data by merging previous values with new updates
            option_data = previous_values.copy()

            # Field mapping for LEVELONE_OPTIONS
            field_mapping = {
                '0': 'symbol',
                '1': 'description',
                '2': 'bid_price',
                '3': 'ask_price',
                '4': 'last_price',
                '5': 'high_price',
                '6': 'low_price',
                '7': 'close_price',
                '8': 'total_volume',
                '9': 'open_interest',
                '10': 'volatility',
                '11': 'money_intrinsic_value',
                '12': 'expiration_year',
                '13': 'multiplier',
                '14': 'digits',
                '15': 'open_price',
                '16': 'bid_size',
                '17': 'ask_size',
                '18': 'last_size',
                '19': 'net_change',
                '20': 'strike_price',
                '21': 'contract_type',
                '22': 'underlying',
                '23': 'expiration_month',
                '24': 'deliverables',
                '25': 'time_value',
                '26': 'expiration_day',
                '27': 'days_to_expiration',
                '28': 'delta',
                '29': 'gamma',
                '30': 'theta',
                '31': 'vega',
                '32': 'rho',
                '33': 'security_status',
                '34': 'theoretical_option_value',
                '35': 'underlying_price',
                '36': 'uv_expiration_type',
                '37': 'mark_price',
                '38': 'quote_time',
                '39': 'trade_time',
                '40': 'exchange',
                '41': 'exchange_name',
                '42': 'last_trading_day',
                '43': 'settlement_type',
                '44': 'net_percent_change',
                '45': 'mark_price_net_change',
                '46': 'mark_price_percent_change',
                '47': 'implied_yield',
                '48': 'is_penny_pilot',
                '49': 'option_root',
                '50': 'week_52_high',
                '51': 'week_52_low',
                '52': 'indicative_ask_price',
                '53': 'indicative_bid_price',
                '54': 'indicative_quote_time',
                '55': 'exercise_type'
            }

            # Update fields that are present in the new message - capture ALL fields
            for field_key, field_name in field_mapping.items():
                if field_key in content_item:
                    value = content_item[field_key]
                    # Convert to appropriate type
                    if field_name in ['bid_price', 'ask_price', 'last_price', 'high_price', 'low_price',
                                      'close_price', 'volatility', 'money_intrinsic_value', 'strike_price',
                                      'time_value', 'delta', 'gamma', 'theta', 'vega', 'rho',
                                      'theoretical_option_value', 'underlying_price', 'mark_price',
                                      'net_percent_change', 'mark_price_net_change', 'mark_price_percent_change',
                                      'implied_yield', 'week_52_high', 'week_52_low', 'indicative_ask_price',
                                      'indicative_bid_price']:
                        try:
                            option_data[field_name] = float(
                                value) if value else 0
                        except (ValueError, TypeError):
                            option_data[field_name] = 0
                    elif field_name in ['total_volume', 'open_interest', 'expiration_year', 'multiplier',
                                        'digits', 'open_price', 'bid_size', 'ask_size', 'last_size',
                                        'expiration_month', 'expiration_day', 'days_to_expiration',
                                        'last_trading_day', 'quote_time', 'trade_time', 'indicative_quote_time']:
                        try:
                            option_data[field_name] = int(
                                value) if value else 0
                        except (ValueError, TypeError):
                            option_data[field_name] = 0
                    elif field_name == 'is_penny_pilot':
                        option_data[field_name] = bool(
                            value) if value else False
                    else:
                        option_data[field_name] = str(value) if value else ''

            # Capture ANY additional fields from the API that aren't in our mapping (preserve all data)
            for field_key, field_value in content_item.items():
                if field_key not in field_mapping and field_key != 'key':
                    # Keep any unmapped fields as-is
                    option_data[f'field_{field_key}'] = field_value

            # Store the updated values for next time
            self.previous_option_values[symbol] = option_data.copy()

            return option_data

        except Exception as e:
            if self.debug:
                print(f"❌ Error parsing option data: {e}")
            return {}

    @staticmethod
    def _clean_symbol_filename(symbol: str) -> str:
        return symbol.replace(' ', '').replace('/', '_').replace('\\', '_')

    @staticmethod
    def _align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
        """Cast/reindex columns to match an existing ParquetWriter schema."""
        columns = []
        for field in schema:
            if field.name in table.column_names:
                col = table.column(field.name)
                try:
                    columns.append(col.cast(field.type, safe=False))
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError, TypeError):
                    # Fall back to Python values then rebuild array
                    columns.append(
                        pa.array(col.to_pylist(), type=field.type))
            else:
                columns.append(pa.nulls(table.num_rows, type=field.type))
        return pa.Table.from_arrays(columns, schema=schema)

    def _queue_parquet_row(self, kind: str, symbol: str, data: dict):
        """Buffer one row and flush to Parquet when the batch is full."""
        clean_symbol = self._clean_symbol_filename(symbol)
        os.makedirs(f'{self.data_output_dir}/{kind}', exist_ok=True)
        path = f'{self.data_output_dir}/{kind}/{clean_symbol}.parquet'
        row = dict(data)

        with self._parquet_lock:
            buffer = self._parquet_buffers.setdefault(path, [])
            buffer.append(row)
            if len(buffer) >= self.parquet_flush_rows:
                self._flush_parquet_path_locked(path)

    def _flush_parquet_path_locked(self, path: str):
        """Flush one symbol buffer to disk. Caller must hold _parquet_lock."""
        rows = self._parquet_buffers.get(path) or []
        if not rows:
            return

        self._parquet_buffers[path] = []
        table = pa.Table.from_pandas(pd.DataFrame(rows), preserve_index=False)
        writer = self._parquet_writers.get(path)

        try:
            if writer is None:
                writer = pq.ParquetWriter(
                    path,
                    table.schema,
                    compression=self.parquet_compression,
                )
                self._parquet_writers[path] = writer
            else:
                table = self._align_table_to_schema(table, writer.schema)
            writer.write_table(table)
        except Exception:
            # Put rows back so a later flush / disconnect can retry
            self._parquet_buffers[path] = rows + self._parquet_buffers.get(
                path, [])
            raise

        if self.debug:
            print(f"💾 Flushed {len(rows)} rows → {path}")

    def flush_parquet(self):
        """Flush all buffered Parquet rows to disk."""
        with self._parquet_lock:
            for path in list(self._parquet_buffers.keys()):
                if self._parquet_buffers.get(path):
                    self._flush_parquet_path_locked(path)

    def close_parquet_writers(self):
        """Flush buffers and close all open Parquet writers."""
        with self._parquet_lock:
            for path in list(self._parquet_buffers.keys()):
                if self._parquet_buffers.get(path):
                    try:
                        self._flush_parquet_path_locked(path)
                    except Exception as e:
                        print(f"❌ Error flushing Parquet {path}: {e}")
            for path, writer in list(self._parquet_writers.items()):
                try:
                    writer.close()
                except Exception as e:
                    print(f"❌ Error closing Parquet writer {path}: {e}")
            self._parquet_writers.clear()

    def save_chart_data_to_csv(self, symbol: str, data: dict):
        """Buffer a chart row and write to Parquet (method name kept for compatibility)."""
        try:
            if 'time' in data and isinstance(data['time'], int) and data['time'] > 0:
                try:
                    timestamp_dt = pd.Timestamp(
                        data['time'], unit='ms', tz='US/Eastern')
                    data['time_et'] = timestamp_dt.strftime(
                        '%Y-%m-%d %H:%M:%S %Z')  # type: ignore
                except (ValueError, OverflowError, AttributeError):
                    pass

            self._queue_parquet_row('equity', symbol, data)
        except Exception as e:
            print(f"❌ Error saving chart data for {symbol}: {e}")

    def save_options_data_to_csv(self, symbol: str, data: dict):
        """Buffer an option row and write to Parquet (method name kept for compatibility)."""
        try:
            timestamp_ms = 0
            if 'indicative_quote_time' in data and isinstance(data['indicative_quote_time'], int) and data['indicative_quote_time'] > 0:
                timestamp_ms = data['indicative_quote_time']
            elif 'quote_time' in data and isinstance(data['quote_time'], int) and data['quote_time'] > 0:
                timestamp_ms = data['quote_time']
            elif 'trade_time' in data and isinstance(data['trade_time'], int) and data['trade_time'] > 0:
                timestamp_ms = data['trade_time']

            if timestamp_ms > 0:
                try:
                    timestamp_dt = pd.Timestamp(
                        timestamp_ms, unit='ms', tz='US/Eastern')
                    data['time_et'] = timestamp_dt.strftime(
                        '%Y-%m-%d %H:%M:%S %Z')  # type: ignore
                except (ValueError, OverflowError, AttributeError):
                    pass

            self._queue_parquet_row('options', symbol, data)
        except Exception as e:
            print(f"❌ Error saving options data for {symbol}: {e}")

    def connect(self, run_until_close: bool = True):
        """
        Connect to WebSocket and log in.

        Args:
            run_until_close: If True (default), block until market close or disconnect.
                             If False, return after successful login (for tests).
        """
        try:
            # Get streamer info from user preferences
            if not self.streamer_info:
                raise Exception("No streamer info available")

            # Get WebSocket URL
            ws_url = self.streamer_info.get('streamerSocketUrl')
            if not ws_url:
                raise Exception("No WebSocket URL in streamer info")

            print(f"🔌 Connecting to WebSocket: {ws_url}")

            # Create WebSocket connection
            self.ws = websocket.WebSocketApp(
                ws_url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
                on_open=self.on_open
            )

            # Start WebSocket connection in a separate thread
            def run_ws():
                if self.ws:
                    self.ws.run_forever()

            # Start the WebSocket thread
            ws_thread = threading.Thread(target=run_ws)
            ws_thread.daemon = True
            ws_thread.start()

            # Wait for connection and login
            timeout = 30
            start_time = time.time()
            while not self.connected and time.time() - start_time < timeout:
                time.sleep(0.1)

            if not self.connected:
                raise Exception(
                    "Failed to connect and login to WebSocket within timeout")

            print("✅ Successfully connected and logged in to WebSocket")

            if not run_until_close:
                return

            # Keep the main thread alive and check for market close
            while self.running and self.connected:
                # Check if it's after market close (4:00:30 PM ET)
                if self.is_after_market_close():
                    print(
                        "🕐 Market close time (4:00:30 PM ET) reached, disconnecting...")
                    self.disconnect()
                    break
                self.flush_parquet()
                time.sleep(1)

        except Exception as e:
            print(f"❌ WebSocket error: {str(e)}")
            if self.ws:
                self.ws.close()
            raise

    def disconnect(self):
        """Disconnect from WebSocket API and flush Parquet buffers."""
        self.running = False
        if self.ws:
            self.ws.close()
        self.connected = False
        self.close_parquet_writers()
        print("🔌 Disconnected from Schwab Streaming API")

    def on_open(self, _):
        """Handle WebSocket connection open"""
        print("🔗 WebSocket connected, attempting login...")
        self.running = True
        self.login()  # Send login request immediately after connection

    def login(self):
        """Send login request to WebSocket"""
        try:
            # Read token from file
            access_token = self.get_access_token()
            if not access_token:
                raise Exception("Failed to get valid access token")

            if self.streamer_info is not None:
                # Prepare login request
                request = {
                    "service": "ADMIN",
                    "command": "LOGIN",
                    "requestid": str(self.request_id),
                    "SchwabClientCustomerId": self.schwab_websocket_client_customer_id,
                    "SchwabClientCorrelId": self.streamer_info.get("schwabClientCorrelId", ""),
                    "parameters": {
                        "Authorization": access_token,
                        "SchwabClientChannel": self.streamer_info.get("schwabClientChannel", ""),
                        "SchwabClientFunctionId": self.streamer_info.get("schwabClientFunctionId", "")
                    }
                }

                if self.debug:
                    print(f"📤 Sending login: {json.dumps(request, indent=2)}")

                # Send login request
                if self.ws:
                    self.ws.send(json.dumps(request))
                self.request_id += 1

        except Exception as e:
            print(f"❌ Login error: {str(e)}")
            raise

    def _register_subscription_wait(self, request_id: int) -> threading.Event:
        """Register a wait event for a subscription request id."""
        event = threading.Event()
        with self._subscription_lock:
            self._subscription_events[request_id] = event
            self._subscription_results.pop(request_id, None)
        return event

    def wait_for_subscription_response(
            self, request_id: int, timeout: float = 15.0) -> Optional[Dict[str, Any]]:
        """
        Block until the streamer responds to a SUBS/ADD/UNSUBS request.

        Returns the response content dict (includes code/msg), or None on timeout.
        """
        with self._subscription_lock:
            event = self._subscription_events.get(request_id)
            if event is None:
                event = threading.Event()
                self._subscription_events[request_id] = event

        if not event.wait(timeout=timeout):
            return None

        with self._subscription_lock:
            return self._subscription_results.pop(request_id, None)

    def _send_options_command(
            self, command: str, symbols: List[str],
            wait_for_response: bool = False,
            timeout: float = 15.0) -> Optional[Dict[str, Any]]:
        """
        Send LEVELONE_OPTIONS SUBS or ADD for the given option symbols.

        SUBS replaces the full options subscription set.
        ADD appends symbols without clearing existing ones.
        """
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return None

        if not symbols:
            print("⚠️ No symbols provided for options data subscription")
            return None

        request_id = self.request_id
        request = {
            "service": "LEVELONE_OPTIONS",
            "command": command,
            "requestid": request_id,
            "SchwabClientCustomerId": self.schwab_websocket_client_customer_id,
            "SchwabClientCorrelId": f"option_{int(time.time() * 1000)}",
            "parameters": {
                "keys": ",".join(symbols),
                # All available LEVELONE_OPTIONS fields
                "fields": "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55"
            }
        }

        event = None
        if wait_for_response:
            event = self._register_subscription_wait(request_id)

        if self.debug:
            print(
                f"📤 Sending LEVELONE_OPTIONS {command} "
                f"({len(symbols)} symbols): {json.dumps(request, indent=2)}")

        if self.ws:
            self.ws.send(json.dumps(request))
        self.request_id += 1

        print(
            f"📤 Sent LEVELONE_OPTIONS {command} for {len(symbols)} contracts "
            f"(requestid={request_id})")

        if wait_for_response and event is not None:
            result = self.wait_for_subscription_response(
                request_id, timeout=timeout)
            if result is None:
                print(
                    f"⚠️ Timed out waiting for LEVELONE_OPTIONS {command} "
                    f"response (requestid={request_id})")
                return None

            if result.get("code") == STREAM_CODE_SUCCESS:
                if command == "SUBS":
                    self.subscriptions["LEVELONE_OPTIONS"] = list(symbols)
                    self.option_symbols = list(symbols)
                elif command == "ADD":
                    existing = self.subscriptions.get("LEVELONE_OPTIONS", [])
                    merged = list(dict.fromkeys(existing + list(symbols)))
                    self.subscriptions["LEVELONE_OPTIONS"] = merged
                    self.option_symbols = merged
            return result

        # Fire-and-forget (legacy streaming path): update bookkeeping immediately
        if command == "SUBS":
            self.subscriptions["LEVELONE_OPTIONS"] = list(symbols)
            self.option_symbols = list(symbols)
        elif command == "ADD":
            existing = self.subscriptions.get("LEVELONE_OPTIONS", [])
            merged = list(dict.fromkeys(existing + list(symbols)))
            self.subscriptions["LEVELONE_OPTIONS"] = merged
            self.option_symbols = merged
        return None

    def subscribe_chart_data(self, symbols: List[str]):
        """Subscribe to CHART_EQUITY data for the given symbols"""
        if not self.connected:
            print("❌ Not connected to WebSocket")
            return

        if not symbols:
            print("⚠️ No symbols provided for chart data subscription")
            return

        try:
            # Prepare the subscription request
            request = {
                "service": "CHART_EQUITY",
                "command": "SUBS",
                "requestid": self.request_id,
                "SchwabClientCustomerId": self.schwab_websocket_client_customer_id,
                "SchwabClientCorrelId": f"chart_{int(time.time() * 1000)}",
                "parameters": {
                    "keys": ",".join(symbols),
                    # All fields: key,sequence,open,high,low,close,volume,time,chart_day
                    "fields": "0,1,2,3,4,5,6,7,8"
                }
            }

            if self.debug:
                print(
                    f"📤 Sending CHART_EQUITY subscription request: {json.dumps(request, indent=2)}")

            # Send the subscription request
            if self.ws:
                self.ws.send(json.dumps(request))
            self.request_id += 1

            # Store the subscription
            self.subscriptions["CHART_EQUITY"] = symbols

            print(
                f"✅ Subscribed to CHART_EQUITY data for: {', '.join(symbols)}")

        except Exception as e:
            print(f"❌ Error subscribing to CHART_EQUITY data: {e}")
            raise

    def subscribe_options_data(
            self, symbols: List[str], wait_for_response: bool = False,
            timeout: float = 15.0) -> Optional[Dict[str, Any]]:
        """Subscribe to LEVELONE_OPTIONS data (SUBS replaces prior options keys)."""
        try:
            return self._send_options_command(
                "SUBS", symbols,
                wait_for_response=wait_for_response, timeout=timeout)
        except Exception as e:
            print(f"❌ Error subscribing to LEVELONE_OPTIONS data: {e}")
            raise

    def add_options_data(
            self, symbols: List[str], wait_for_response: bool = False,
            timeout: float = 15.0) -> Optional[Dict[str, Any]]:
        """Add LEVELONE_OPTIONS symbols without clearing existing subscriptions."""
        try:
            return self._send_options_command(
                "ADD", symbols,
                wait_for_response=wait_for_response, timeout=timeout)
        except Exception as e:
            print(f"❌ Error adding LEVELONE_OPTIONS data: {e}")
            raise

    def on_message(self, _, message):
        """Handle WebSocket messages"""
        try:
            data = json.loads(message)

            if self.debug:
                print(f"📨 Received message: {json.dumps(data, indent=2)}")

            # Handle different message types
            if isinstance(data, dict):
                if "response" in data:
                    for response_data in data["response"]:
                        command = response_data.get("command")
                        content = response_data.get("content", {})
                        code = content.get("code", -1)
                        msg = content.get("msg", "")
                        service = response_data.get("service", "")

                        # Normalize request id (streamer may send int or str)
                        raw_request_id = response_data.get("requestid")
                        try:
                            request_id = int(raw_request_id) if raw_request_id is not None else None
                        except (TypeError, ValueError):
                            request_id = None

                        if command == "LOGIN":
                            if code == STREAM_CODE_SUCCESS:
                                print("✅ WebSocket login successful")
                                if "status=" in msg:
                                    status = msg.split("status=")[1].split(
                                        ";")[0] if ";" in msg else msg.split("status=")[1]
                                    print(f"📊 Account status: {status}")
                                self.connected = True

                                if self.auto_subscribe:
                                    print(
                                        "📊 Subscribing to chart and options data...")
                                    if self.symbols:
                                        self.subscribe_chart_data(self.symbols)
                                    else:
                                        print(
                                            "⚠️ No equity symbols loaded, skipping chart data subscription")

                                    if self.option_symbols:
                                        self.subscribe_options_data(
                                            self.option_symbols)
                                    else:
                                        print(
                                            "⚠️ No option symbols loaded, skipping options data subscription")
                                else:
                                    print(
                                        "ℹ️ auto_subscribe=False — waiting for manual subscriptions")
                            else:
                                print(
                                    f"❌ WebSocket login failed with code: {code}")
                                print(
                                    f"   Message: {content.get('msg', 'Unknown error')}")
                                self.connected = False

                        elif command in ("SUBS", "ADD", "UNSUBS", "VIEW"):
                            if code == STREAM_CODE_SUCCESS:
                                print(
                                    f"✅ {service} {command} accepted "
                                    f"(requestid={request_id})")
                            elif code == STREAM_CODE_REACHED_SYMBOL_LIMIT:
                                print(
                                    f"🚫 {service} {command} hit REACHED_SYMBOL_LIMIT "
                                    f"(code={code}, requestid={request_id}): {msg}")
                            else:
                                print(
                                    f"⚠️ {service} {command} response "
                                    f"code={code} requestid={request_id}: {msg}")

                            if request_id is not None:
                                with self._subscription_lock:
                                    self._subscription_results[request_id] = {
                                        "service": service,
                                        "command": command,
                                        "code": code,
                                        "msg": msg,
                                        "content": content,
                                    }
                                    event = self._subscription_events.get(
                                        request_id)
                                    if event:
                                        event.set()

                elif "notify" in data:
                    # Heartbeat or other notifications
                    notify_data = data["notify"][0]
                    if notify_data.get("heartbeat"):
                        if self.debug:
                            print("💓 Heartbeat received")
                    elif notify_data.get("service") == "ADMIN":
                        content = notify_data.get("content", {})
                        if content.get("code") == 30:  # Empty subscription
                            if self.auto_subscribe:
                                print(
                                    "⚠️ Empty subscription detected, resubscribing...")
                                if self.symbols:
                                    self.subscribe_chart_data(self.symbols)
                                if self.option_symbols:
                                    self.subscribe_options_data(
                                        self.option_symbols)
                            else:
                                print(
                                    "⚠️ Empty subscription detected "
                                    "(auto_subscribe=False, not resubscribing)")

                elif "data" in data:
                    # Market data
                    for data_item in data["data"]:
                        service = data_item.get("service")
                        content = data_item.get("content", [])

                        if service == "CHART_EQUITY":
                            if self.debug:
                                print(
                                    f"📈 Received CHART_EQUITY data: {len(content)} items")
                            # Process and save chart data
                            for candle_data in content:
                                if self.debug:
                                    print(f"   Candle data: {candle_data}")

                                # Parse the chart data
                                parsed_candle = self.parse_chart_data(
                                    candle_data)
                                if parsed_candle and 'key' in parsed_candle:
                                    symbol = parsed_candle.get('key')
                                    # Only save if symbol is in our subscription list
                                    if symbol in self.symbols:
                                        # Save to CSV
                                        self.save_chart_data_to_csv(
                                            symbol, parsed_candle)
                                    elif self.debug:
                                        print(
                                            f"⚠️ Symbol {symbol} not in subscription list, skipping save")

                        elif service == "LEVELONE_OPTIONS":
                            if self.debug:
                                print(
                                    f"📊 Received LEVELONE_OPTIONS data: {len(content)} items")
                            # Process and save options data
                            for option_data in content:
                                if self.debug:
                                    print(f"   Option data: {option_data}")

                                # Parse the option data
                                parsed_option = self.parse_option_data(
                                    option_data)
                                if parsed_option:
                                    # Get symbol from parsed data or raw data
                                    symbol = parsed_option.get(
                                        'symbol', '') or option_data.get('key', '')
                                    if not symbol:
                                        continue

                                    # Normalize symbols for comparison (remove spaces)
                                    normalized_symbol = symbol.replace(' ', '')
                                    normalized_option_symbols = [
                                        s.replace(' ', '') for s in self.option_symbols]

                                    # Only save if symbol matches any in our subscription list
                                    if normalized_symbol in normalized_option_symbols or any(norm_sym in normalized_symbol for norm_sym in normalized_option_symbols):
                                        if self.on_option_update:
                                            try:
                                                self.on_option_update(
                                                    symbol, parsed_option)
                                            except Exception as cb_err:
                                                if self.debug:
                                                    print(
                                                        f"⚠️ on_option_update error: {cb_err}")
                                        # Save to CSV (save whenever we have valid data)
                                        if parsed_option.get('last_price', 0) != 0 or parsed_option.get('bid_price', 0) != 0:
                                            self.save_options_data_to_csv(
                                                symbol, parsed_option)
                                    elif self.debug:
                                        print(
                                            f"⚠️ Symbol {symbol} not in option subscription list, skipping save")

                else:
                    # Handle unexpected message types
                    if self.debug:
                        print(
                            f"⚠️ Unexpected message type received: {list(data.keys())}")

        except Exception as e:
            print(f"❌ Message handling error: {str(e)}")
            if self.debug:
                print(f"   Raw message: {message}")
            raise

    def on_error(self, _, error):
        """Handle WebSocket errors"""
        print(f"❌ WebSocket error: {str(error)}")
        self.connected = False

    def on_close(self, _, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        print(
            f"WebSocket connection closed: {close_status_code} - {close_msg}")
        self.connected = False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='schwab-websocket-stream-capture: stream Schwab market data to Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python schwab_websocket_client.py
  python schwab_websocket_client.py --symbols my_symbols.txt --options my_options.txt
  python schwab_websocket_client.py --option-chain SPY --min-dte 10
  python schwab_websocket_client.py --option-chain SPY --min-dte 10 --strike-range 25
  python schwab_websocket_client.py --option-chain SPY --min-dte 0 --data-dir ./spy_0dte
  python schwab_websocket_client.py --data-dir /path/to/data --debug
        '''
    )
    parser.add_argument('--symbols', '-s', type=str, default=DEFAULT_EQUITY_SYMBOLS_FILE,
                        help=f'Equity symbols file, comma-separated (default: {DEFAULT_EQUITY_SYMBOLS_FILE})')
    parser.add_argument('--options', '-o', type=str, default=DEFAULT_OPTION_SYMBOLS_FILE,
                        help=f'Option symbols file, comma-separated OSI contracts (default: {DEFAULT_OPTION_SYMBOLS_FILE})')
    parser.add_argument('--option-chain', type=str, default=None,
                        help='Underlying ticker: fetch and stream the option chain '
                             '(earliest expiration with DTE >= --min-dte)')
    parser.add_argument('--min-dte', type=int, default=0,
                        help='With --option-chain: earliest expiration whose DTE is >= this '
                             '(default: 0 / 0DTE)')
    parser.add_argument('--strike-range', type=int, default=0,
                        help='With --option-chain: ATM center +/- N strikes '
                             '(default: 0 = entire strike chain)')
    parser.add_argument('--contract-type', choices=['ALL', 'CALL', 'PUT'], default='ALL',
                        help='With --option-chain: CALL, PUT, or ALL (default: ALL)')
    parser.add_argument('--batch-size', type=int, default=50,
                        help='With --option-chain: contracts per SUBS/ADD batch (default: 50)')
    parser.add_argument('--token', '-t', type=str, default=DEFAULT_ACCESS_TOKEN_FILE,
                        help=f'Access token file, raw token on one line (default: {DEFAULT_ACCESS_TOKEN_FILE})')
    parser.add_argument('--data-dir', '-d', type=str, default=DEFAULT_DATA_OUTPUT_DIR,
                        help=f'Output directory for .parquet files (default: {DEFAULT_DATA_OUTPUT_DIR})')
    parser.add_argument('--parquet-flush-rows', type=int, default=DEFAULT_PARQUET_FLUSH_ROWS,
                        help=f'Rows buffered per symbol before Parquet flush '
                             f'(default: {DEFAULT_PARQUET_FLUSH_ROWS})')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode')

    args = parser.parse_args()

    if args.min_dte < 0:
        raise SystemExit('--min-dte must be >= 0')
    if args.strike_range < 0:
        raise SystemExit('--strike-range must be >= 0 (0 = entire chain)')
    if args.batch_size < 1:
        raise SystemExit('--batch-size must be >= 1')
    if args.parquet_flush_rows < 1:
        raise SystemExit('--parquet-flush-rows must be >= 1')

    chain_mode = args.option_chain is not None
    if chain_mode:
        underlying = args.option_chain.upper().strip()
        if not underlying:
            raise SystemExit('--option-chain requires a ticker symbol (e.g. SPY)')
        client = SchwabWebSocketClient(
            debug=args.debug,
            access_token_filepath=args.token,
            data_output_dir=args.data_dir,
            equity_symbols=[],
            option_symbols=[],
            auto_subscribe=False,
            parquet_flush_rows=args.parquet_flush_rows,
        )
    else:
        client = SchwabWebSocketClient(
            debug=args.debug,
            symbols_filepath=args.symbols,
            option_symbols_filepath=args.options,
            access_token_filepath=args.token,
            data_output_dir=args.data_dir,
            parquet_flush_rows=args.parquet_flush_rows,
        )

    try:
        if not client.wait_for_market_open():
            print("❌ Market is closed (weekend). Exiting...")
            raise SystemExit(0)

        if chain_mode:
            contracts, expiration_date, dte = client.load_option_chain(
                underlying,
                min_dte=args.min_dte,
                contract_type=args.contract_type,
                strike_range=args.strike_range if args.strike_range > 0 else None,
            )
            range_desc = (
                f"ATM +/- {args.strike_range}"
                if args.strike_range > 0
                else "entire chain"
            )
            print(
                f"📊 Option-chain mode: {underlying} {expiration_date} "
                f"(DTE={dte}, {range_desc}) — {len(contracts)} contracts "
                f"→ {args.data_dir}/options/*.parquet")

        print("🔌 Connecting to Schwab Streaming API...")
        if chain_mode:
            client.connect(run_until_close=False)
            accepted = client.subscribe_option_chain(
                contracts, batch_size=args.batch_size)
            if accepted <= 0:
                raise Exception("No option contracts were accepted for streaming")
            print("📡 Streaming option chain until market close (Ctrl+C to stop)...")
            while client.running and client.connected:
                if client.is_after_market_close():
                    print(
                        "🕐 Market close time (4:00:30 PM ET) reached, disconnecting...")
                    client.disconnect()
                    break
                client.flush_parquet()
                time.sleep(1)
        else:
            client.connect()
            while client.running and client.connected:
                if client.is_after_market_close():
                    print(
                        "🕐 Market close time (4:00:30 PM ET) reached, disconnecting...")
                    client.disconnect()
                    break
                client.flush_parquet()
                time.sleep(1)

        print("✅ Streaming session completed")

    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
        client.disconnect()
    except Exception as e:
        print(f"❌ Error: {e}")
        client.disconnect()
        raise SystemExit(1) from e
