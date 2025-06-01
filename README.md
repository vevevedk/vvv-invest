# VVV Invest Data Collection System

A system for collecting and analyzing dark pool trades and news headlines for financial market analysis.

## Features

- Dark pool trade collection and analysis
- News headline collection and sentiment analysis
- Data export to CSV for further analysis
- Support for both real-time collection and historical backfill

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure database settings in `config/db_config.py`

3. Configure API settings in `config/api_config.py`

## Usage

### Real-time Collection

Run the collectors to gather the latest data:
```bash
python3 scripts/run_collectors.py
```

This will:
- Collect recent dark pool trades
- Collect latest news headlines
- Export data to CSV files in the `exports` directory

### Historical Backfill

To backfill historical data:
```bash
# Default 7-day backfill
python3 scripts/run_backfill.py

# Custom number of days
python3 scripts/run_backfill.py --days 14

# Specific symbols
python3 scripts/run_backfill.py --symbols AAPL MSFT GOOGL
```

## Project Structure

- `collectors/` - Data collection modules
  - `darkpool/` - Dark pool trade collection
  - `news/` - News headline collection
  - `utils/` - Shared utilities
- `config/` - Configuration files
- `scripts/` - Command-line scripts
- `exports/` - Exported data files
- `logs/` - Log files

## Logging

Logs are stored in the `logs/` directory:
- `collector/darkpool_collector.log`
- `collector/news_collector.log`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
