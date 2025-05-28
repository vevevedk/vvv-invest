# Dark Pool Data Exploration with Unusual Whales API

## Overview
This document summarizes our exploration of the Unusual Whales dark pool API, focusing on retrieving and analyzing dark pool trades for SPY and QQQ.

## API Endpoints Explored

### 1. `/darkpool/recent`
- Returns recent dark pool trades
- Maximum limit: 200 trades per request
- Parameters:
  - `limit`: Number of trades to return (max 200)
  - `date`: Date in YYYY-MM-DD format
  - `offset`: Pagination parameter (not functioning as expected)
  - `sort`: Sorting parameter (not functioning as expected)

### 2. `/darkpool/{ticker}`
- Returns dark pool trades for a specific ticker
- Maximum limit: 500 trades per request
- Parameters:
  - `limit`: Number of trades to return (max 500)
  - `date`: Date in YYYY-MM-DD format

## Key Findings

### API Limitations
1. **Pagination Issues**:
   - The `offset` parameter does not work as expected
   - Same trades are returned regardless of offset value
   - No way to get total count of available trades

2. **Sorting Issues**:
   - The `sort` parameter does not affect results
   - Trades are returned in the same order regardless of sort direction

3. **Rate Limits**:
   - API has rate limiting that needs to be respected
   - Implemented rate limiting in our scripts

### Data Structure
Each trade contains:
- `size`: Trade size
- `price`: Trade price
- `volume`: Volume
- `premium`: Premium amount
- `executed_at`: Execution timestamp
- `nbbo_ask`: National Best Bid and Offer ask price
- `nbbo_bid`: National Best Bid and Offer bid price
- `market_center`: Trading venue
- `tracking_id`: Unique identifier for the trade
- `sale_cond_codes`: Special condition codes

## Approaches Tried

### 1. Direct API Fetching
- Attempted to fetch all trades for a given date
- Limited by API's 200/500 trade limits
- No effective pagination mechanism
- Results in incomplete data for busy trading days

### 2. Continuous Collection Script
Created `darkpool_collector.py`:
- Runs every 5 minutes during market hours
- Collects up to 200 trades each time
- Saves to hourly CSV files
- Implements deduplication using `tracking_id`
- Handles market hours and weekends
- Includes rate limiting and error handling

### 3. Analysis Scripts
Developed analysis tools for:
- Volume analysis
- Price impact calculations
- Block trade identification
- Market center distribution
- Time-based patterns

## Recommended Approach

Given the API limitations, the most effective approach is:

1. **Continuous Data Collection**:
   - Run a collector service on a cloud server (e.g., Digital Ocean droplet)
   - Collect trades every 5 minutes during market hours
   - Store in a database for efficient querying
   - Implement proper deduplication

2. **Web Application**:
   - Build a web interface for:
     - Real-time trade monitoring
     - Historical analysis
     - Custom reporting
     - Alert configuration

3. **Analysis Pipeline**:
   - Process collected data in batches
   - Generate daily/weekly/monthly reports
   - Implement custom analytics
   - Create visualizations

## Next Steps

1. **Web Application Development**:
   - Set up cloud infrastructure
   - Implement continuous data collection
   - Design database schema
   - Create web interface

2. **Analysis Features**:
   - Real-time trade monitoring
   - Historical data analysis
   - Custom report generation
   - Alert system

3. **Data Quality**:
   - Implement data validation
   - Handle API changes
   - Monitor data completeness
   - Regular data backups

## Conclusion

The Unusual Whales dark pool API provides valuable data but has significant limitations in terms of data retrieval. A continuous collection approach with a web application interface is the most effective way to build a comprehensive dark pool analysis system. 