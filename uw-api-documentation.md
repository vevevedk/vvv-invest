# Unusual Whales API Documentation for Options Flow Collection

## Authentication
All requests require a bearer token in the Authorization header:
```
Authorization: Bearer YOUR_TOKEN
```

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