# Unusual Whales API Documentation for Options Flow Collection

## Authentication
All requests require a bearer token in the Authorization header:
```
Authorization: Bearer YOUR_TOKEN
```

## Darkpool Endpoints

### 1. Recent Darkpool Trades
`GET https://api.unusualwhales.com/api/darkpool/recent`

Returns the latest darkpool trades.

**Query Parameters:**
- `date` (string, optional): Trading date in YYYY-MM-DD format. Defaults to last trading date.
- `limit` (integer, optional): Number of items to return. Default: 100. Max: 200. Min: 1.
- `max_premium` (integer, optional): Maximum premium for trades.
- `max_size` (integer, optional): Maximum size for trades.
- `max_volume` (integer, optional): Maximum volume for trades.
- `min_premium` (integer, optional): Minimum premium for trades. Default: 0.
- `min_size` (integer, optional): Minimum size for trades. Default: 0.
- `min_volume` (integer, optional): Minimum volume for trades. Default: 0.

**Response Fields:**
- `canceled` (boolean): Whether the trade has been cancelled.
- `executed_at` (string): Time with timezone when trade was executed. Example: "2023-02-16T00:59:44Z"
- `ext_hour_sold_codes` (string|null): Code for out-of-regular-hours trade. Allowed values: sold_out_of_sequence, extended_hours_trade_late_or_out_of_sequence, extended_hours_trade. Example: "extended_hours_trade"
- `market_center` (string): Market center code. Example: "L"
- `nbbo_ask` (string|number): NBBO ask price. Example: "19"
- `nbbo_ask_quantity` (integer|number): NBBO ask quantity. Example: 6600
- `nbbo_bid` (string|number): NBBO bid price. Example: "18.99"
- `nbbo_bid_quantity` (integer|number): NBBO bid quantity. Example: 29100
- `premium` (string): Total premium. Example: "121538.56"
- `price` (string): Trade price. Example: "18.9904"
- `sale_cond_codes` (string|null): Sale condition code. Allowed values: contingent_trade, odd_lot_execution, prio_reference_price, average_price_trade. Example: "contingent_trade"
- `size` (integer): Transaction size. Example: 6400
- `ticker` (string): Stock ticker. Example: "AAPL"
- `tracking_id` (integer): Trade tracking ID. Example: 71984388012245
- `trade_code` (string|null): Trade code. Allowed values: derivative_priced, qualified_contingent_trade, intermarket_sweep. Example: "derivative_priced"
- `trade_settlement` (string|null): Trade settlement type. Allowed values: cash_settlement, next_day_settlement, seller_settlement, regular_settlement. Example: "cash_settlement"
- `volume` (integer): Ticker volume for the trading day. Example: 23132119

### 2. Ticker Darkpool Trades
`GET https://api.unusualwhales.com/api/darkpool/{ticker}`

Returns darkpool trades for a given ticker on a given day.

**Path Parameters:**
- `ticker` (string, required): Stock ticker. Example: "AAPL"

**Query Parameters:**
- `date` (string, optional): Trading date in YYYY-MM-DD format. Defaults to last trading date.
- `limit` (integer, optional): Number of items to return. Default: 500. Max: 500. Min: 1.
- `max_premium`, `max_size`, `max_volume`, `min_premium`, `min_size`, `min_volume` (see above)
- `newer_than` (string, optional): Only return results newer than this ISO date or unix time.
- `older_than` (string, optional): Only return results older than this ISO date or unix time.

**Response Fields:**
- Same as for /api/darkpool/recent (see above)

**Notes:**
- The max limit for /recent is 200, for /{ticker} is 500.
- All fields may be null if not applicable.

## Key Endpoints for Options Flow Collection

### 1. Flow Data
`GET https://api.unusualwhales.com/api/option-contract/{id}/flow`

Returns the last 50 option trades for a specific option chain.

**Parameters:**
- `id` (required): Option contract in ISO format (e.g., `TSLA230526P00167500`)
- `date` (optional): Trading date (YYYY-MM-DD), defaults to last trading date
- `limit` (optional): Number of items to return (min: 1)
- `min_premium` (optional): Minimum premium filter (default: 0)
- `side` (optional): Trade side filter (ALL, ASK, BID, MID)

**Key Response Fields:**
```json
{
  "data": [
    {
      "ask_vol": 2,
      "bid_vol": 1,
      "delta": "0.610546281537814",
      "executed_at": "2024-08-21T13:50:52.278302Z",
      "expiry": "2025-01-17",
      "implied_volatility": "0.604347250962543",
      "option_chain_id": "NVDA250117C00124000",
      "option_type": "call",
      "premium": "2150.00",
      "price": "21.50",
      "size": 1,
      "strike": "124.0000000000",
      "underlying_symbol": "NVDA",
      "volume": 33
    }
  ]
}
```

### 2. Option Contracts
`GET https://api.unusualwhales.com/api/stock/{ticker}/option-contracts`

Returns all option contracts for a given ticker.

**Parameters:**
- `ticker` (required): Stock symbol (e.g., `AAPL`)
- `expiry` (optional): Filter by expiry date (YYYY-MM-DD)
- `option_type` (optional): Filter by 'call' or 'put'
- `exclude_zero_dte` (optional): Exclude same-day expiry contracts
- `vol_greater_oi` (optional): Only show contracts where volume > open interest
- `limit` (optional): Number of items to return
- `page` (optional): Pagination support

**Key Response Fields:**
```json
{
  "data": [
    {
      "ask_volume": 119403,
      "bid_volume": 122789,
      "implied_volatility": "0.675815680048166",
      "last_price": "0.03",
      "nbbo_ask": "0.03",
      "nbbo_bid": "0.03",
      "open_interest": 18680,
      "option_symbol": "AAPL230915C00150000",
      "volume": 264899
    }
  ]
}
```

### 3. Expiry Breakdown
`GET https://api.unusualwhales.com/api/stock/{ticker}/expiry-breakdown`

Returns all expirations for a given ticker on a specific trading day.

**Parameters:**
- `ticker` (required): Stock symbol (e.g., `AAPL`)
- `date` (optional): Trading date (YYYY-MM-DD), defaults to last trading date

**Response Example:**
```json
{
  "data": [
    {
      "chains": 5000,
      "expiry": "2023-09-07",
      "open_interest": 554,
      "volume": 1566232
    }
  ]
}
```

### 4. News Headlines
`GET https://api.unusualwhales.com/api/news/headlines`

Returns the latest news headlines for financial markets. This endpoint provides access to news headlines that may impact the markets, including company-specific news, sector news, and market-wide events. Headlines can be filtered by source, content, and significance.

**Request Headers:**
- `Authorization: Bearer <YOUR_TOKEN>`
- `Accept: application/json, text/plain`

**Query Parameters:**
- `limit` (integer): How many items to return. Default: 50. Max: 100. Min: 1. Example: 10
- `major_only` (boolean): When true, only returns major/significant news. Default: false. Example: true
- `page` (integer): Page number (use with limit). Starts on page 0. Example: 1
- `search_term` (string): A search term to filter news headlines by content. Example: earnings
- `sources` (string): A comma-separated list of news sources to filter by (e.g., 'Reuters,Bloomberg'). Example: BusinessWire,MarketNews

**Sample curl request:**
```
curl --request GET \
  --url 'https://api.unusualwhales.com/api/news/headlines?limit=10&page=1&major_only=true&search_term=earnings&sources=BusinessWire,MarketNews' \
  --header 'Accept: application/json, text/plain' \
  --header 'Authorization: Bearer <YOUR_TOKEN>'
```

**Sample Response:**
```json
{
  "data": [
    {
      "created_at": "2023-04-15T16:30:00Z",
      "headline": "Company XYZ Reports Better Than Expected Earnings",
      "is_major": true,
      "meta": {},
      "sentiment": "positive",
      "source": "BusinessWire",
      "tags": ["earnings", "tech"],
      "tickers": ["XYZ", "EXMP"]
    },
    {
      "created_at": "2023-04-15T14:20:00Z",
      "headline": "Federal Reserve Signals Possible Rate Cut",
      "is_major": true,
      "meta": {},
      "sentiment": "neutral",
      "source": "MarketNews",
      "tags": ["federal-reserve", "interest-rates"],
      "tickers": []
    }
  ]
}
```

**Rate Limiting & Pagination:**
- Use the `limit` and `page` parameters for pagination.
- Respect the API's rate limits (see official documentation or contact support for your plan's limits).
- If you receive a 429 error, implement a delay and/or exponential backoff before retrying.

## Implementation Strategy for Options Flow Collector

For our options flow collector, we should:

1. Use the **Expiry Breakdown** endpoint to get active expirations for our target symbols
2. Use the **Option Contracts** endpoint to get all contracts for each expiration
3. Use the **Flow Data** endpoint to collect actual flow data for each contract

### Collection Process:
1. For each symbol in our watchlist:
   - Get expiry dates using expiry-breakdown
   - For each expiry:
     - Get option contracts
     - For each contract:
       - Collect flow data
       - Filter based on our criteria (premium, volume, etc.)
       - Save to database

### Rate Limiting
- Implement appropriate rate limiting between requests
- Use pagination where available
- Cache contract and expiry data to minimize API calls 

## Flow Alerts Endpoint

### 1. Flow Alerts
`GET https://api.unusualwhales.com/api/option-trades/flow-alerts`

Returns the latest flow alerts.

**Request Headers:**
- `Authorization: Bearer <YOUR_TOKEN>`
- `Accept: application/json`

**Query Parameters:**
- `all_opening` (boolean): Whether all transactions are opening transactions based on OI, Size & Volume. Default: true
- `is_ask_side` (boolean): Whether a transaction is ask side. Default: true
- `is_bid_side` (boolean): Whether a transaction is bid side. Default: true
- `is_call` (boolean): Whether a transaction is a call. Default: true
- `is_floor` (boolean): Whether a transaction is from the floor. Default: true
- `is_otm` (boolean): Only include contracts which are currently out of the money
- `is_put` (boolean): Whether a transaction is a put. Default: true
- `is_sweep` (boolean): Whether a transaction is an intermarket sweep. Default: true
- `issue_types[]` (array[string]): Array of issue types. Allowed values: "Common Stock", "ETF", "Index", "ADR"
- `limit` (integer): Number of items to return. Default: 100. Max: 200. Min: 1
- `max_diff` (string): Maximum OTM diff of a contract
- `max_dte` (integer): Maximum days to expiry. Min: 0
- `max_open_interest` (integer): Maximum open interest on alert's contract. Min: 0
- `max_premium` (integer): Maximum premium on alert. Min: 0
- `max_size` (integer): Maximum size on alert. Min: 0
- `max_volume` (integer): Maximum volume on alert's contract. Min: 0
- `max_volume_oi_ratio` (integer): Maximum contract volume to open interest ratio. Min: 0
- `min_diff` (string): Minimum OTM diff of a contract
- `min_dte` (integer): Minimum days to expiry. Min: 0
- `min_open_interest` (integer): Minimum open interest on alert's contract. Min: 0
- `min_premium` (integer): Minimum premium on alert. Min: 0
- `min_size` (integer): Minimum size on alert. Min: 0
- `min_volume` (integer): Minimum volume on alert's contract. Min: 0
- `min_volume_oi_ratio` (integer): Minimum contract volume to open interest ratio. Min: 0
- `newer_than` (string): Unix time or ISO date for pagination
- `older_than` (string): Unix time or ISO date for pagination
- `rule_name[]` (array[string]): Array of rule names. Allowed values:
  - "FloorTradeSmallCap"
  - "FloorTradeMidCap"
  - "RepeatedHits"
  - "RepeatedHitsAscendingFill"
  - "RepeatedHitsDescendingFill"
  - "FloorTradeLargeCap"
  - "OtmEarningsFloor"
  - "LowHistoricVolumeFloor"
  - "SweepsFollowedByFloor"
- `ticker_symbol` (string): Comma-separated list of tickers. Prefix with - to exclude

**Response Fields:**
- `alert_rule` (string): Name of the alert rule
- `all_opening_trades` (boolean): Whether all trades are opening trades
- `created_at` (string): UTC timestamp
- `expiry` (string): Contract expiry date in ISO format
- `expiry_count` (integer): Number of expiries in multileg trade
- `has_floor` (boolean): Whether trade has floor component
- `has_multileg` (boolean): Whether trade is multileg
- `has_singleleg` (boolean): Whether trade is singleleg
- `has_sweep` (boolean): Whether trade is a sweep
- `open_interest` (number): Open interest at time of alert
- `option_chain` (string): Option symbol of contract
- `price` (number): Trade price
- `strike` (string): Contract strike price
- `ticker` (string): Underlying ticker
- `total_ask_side_prem` (number): Total premium on ask side
- `total_bid_side_prem` (number): Total premium on bid side
- `total_premium` (number): Total premium
- `total_size` (number): Total size
- `trade_count` (number): Number of trades
- `type` (string): Contract type ("call" or "put")
- `underlying_price` (number): Price of underlying at time of alert
- `volume` (number): Volume at time of alert
- `volume_oi_ratio` (number): Volume to open interest ratio

**Example Response:**
```json
{
  "data": [
    {
      "alert_rule": "RepeatedHits",
      "all_opening_trades": false,
      "created_at": "2023-12-12T16:35:52.168490Z",
      "expiry": "2023-12-22",
      "expiry_count": 1,
      "has_floor": false,
      "has_multileg": false,
      "has_singleleg": true,
      "has_sweep": true,
      "open_interest": 1234,
      "option_chain": "AAPL231222C00175000",
      "price": 2.50,
      "strike": "175",
      "ticker": "AAPL",
      "total_ask_side_prem": 25000,
      "total_bid_side_prem": 0,
      "total_premium": 25000,
      "total_size": 100,
      "trade_count": 1,
      "type": "call",
      "underlying_price": 178.50,
      "volume": 500,
      "volume_oi_ratio": 0.41
    }
  ]
}
``` 

## Economic Calendar Endpoint

### 1. Economic Calendar
`GET https://api.unusualwhales.com/api/market/economic-calendar`

Returns the economic calendar for the current and next week.

**Request Headers:**
- `Authorization: Bearer <YOUR_TOKEN>`
- `Accept: application/json`

**Response Fields:**
- `event` (string): The event/reason. Can be a fed speaker or an economic report/indicator
- `forecast` (string|null): The forecast if the event is an economic report/indicator
- `prev` (string|null): The previous value of the preceding period if the event is an economic report/indicator
- `reported_period` (string|null): The period for which the economic report/indicator is being reported
- `time` (string): The time at which the event will start as UTC timestamp
- `type` (string): The type of the event. Allowed values: "fed-speaker", "fomc", "report"

**Example Response:**
```json
{
  "data": [
    {
      "event": "Consumer sentiment (final)",
      "forecast": "69.4",
      "prev": "69.4",
      "reported_period": "December",
      "time": "2023-12-22T15:00:00Z",
      "type": "report"
    },
    {
      "event": "PCE index",
      "forecast": null,
      "prev": "0.0%",
      "reported_period": "November",
      "time": "2023-12-22T13:30:00Z",
      "type": "report"
    }
  ]
}
```

**Example curl request:**
```bash
curl --request GET \
  --url 'https://api.unusualwhales.com/api/market/economic-calendar' \
  --header 'Accept: application/json' \
  --header 'Authorization: Bearer <YOUR_TOKEN>'
``` 

## Earnings Endpoints

### 1. Afterhours Earnings
`GET https://api.unusualwhales.com/api/earnings/afterhours`

Returns the afterhours earnings for a given date.

**Request Headers:**
- `Authorization: Bearer <YOUR_TOKEN>`
- `Accept: application/json`

**Query Parameters:**
- `date` (string, optional): Trading date in YYYY-MM-DD format. Defaults to last trading date. Example: 2024-01-18
- `limit` (integer, optional): Number of items to return. Default: 50. Max: 100. Min: 1. Example: 10
- `page` (integer, optional): Page number (use with limit). Starts on page 0. Example: 1

**Response Fields:**
- `actual_eps` (string)
- `continent` (string)
- `country_code` (string)
- `country_name` (string)
- `ending_fiscal_quarter` (string, ISO date)
- `expected_move` (string)
- `expected_move_perc` (string)
- `full_name` (string)
- `has_options` (boolean or null)
- `is_s_p_500` (boolean)
- `marketcap` (string)
- `post_earnings_close` (string)
- `post_earnings_date` (string, ISO date)
- `pre_earnings_close` (string)
- `pre_earnings_date` (string, ISO date)
- `reaction` (string)
- `report_date` (string, ISO date)
- `report_time` (string): premarket, postmarket, or unknown
- `sector` (string)
- `source` (string): company or estimation
- `street_mean_est` (string)
- `symbol` (string)

---

### 2. Premarket Earnings
`GET https://api.unusualwhales.com/api/earnings/premarket`

Returns the premarket earnings for a given date.

**Request Headers:**
- `Authorization: Bearer <YOUR_TOKEN>`
- `Accept: application/json`

**Query Parameters:**
- `date` (string, optional): Trading date in YYYY-MM-DD format. Defaults to last trading date. Example: 2024-01-18
- `limit` (integer, optional): Number of items to return. Default: 50. Max: 100. Min: 1. Example: 10
- `page` (integer, optional): Page number (use with limit). Starts on page 0. Example: 1

**Response Fields:**
- Same as Afterhours Earnings

---

### 3. Historical Ticker Earnings
`GET https://api.unusualwhales.com/api/earnings/{ticker}`

Returns the historical earnings for the given ticker.

**Request Headers:**
- `Authorization: Bearer <YOUR_TOKEN>`
- `Accept: application/json`

**Path Parameters:**
- `ticker` (string, required): A single ticker. Example: AAPL

**Response Fields:**
- `actual_eps` (string)
- `ending_fiscal_quarter` (string, ISO date)
- `expected_move` (string)
- `expected_move_perc` (string)
- `long_straddle_1d` (string)
- `long_straddle_1w` (string)
- `post_earnings_move_1d` (string)
- `post_earnings_move_1w` (string)
- `post_earnings_move_2w` (string)
- `post_earnings_move_3d` (string)
- `pre_earnings_move_1d` (string)
- `pre_earnings_move_1w` (string)
- `pre_earnings_move_2w` (string)
- `pre_earnings_move_3d` (string)
- `report_date` (string, ISO date)
- `report_time` (string): premarket, postmarket, or unknown
- `short_straddle_1d` (string)
- `short_straddle_1w` (string)
- `source` (string): company or estimation
- `street_mean_est` (string) 