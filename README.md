# vvv-invest

A comprehensive market data collection and analysis system for investment research.

## Project Structure

```
vvv-invest/
├── collectors/              # Data collection modules
│   ├── darkpool/           # Dark pool trade collection
│   ├── news/              # News headline collection
│   ├── options/           # Options flow collection
│   └── schema_validation/ # Schema validation utilities
├── config/                # Configuration files
│   ├── celery/           # Celery task configuration
│   ├── logrotate/        # Log rotation configuration
│   └── systemd/          # Systemd service files
├── data/                 # Data storage
│   ├── cache/           # Cached data files
│   ├── processed/       # Processed data files
│   └── raw/            # Raw data files
├── docs/                # Documentation
│   ├── api/            # API documentation
│   ├── deployment/     # Deployment guides
│   ├── development/    # Development guides
│   ├── planning/       # Project planning docs
│   └── work_notes/     # Development work notes
├── flow_analysis/      # Data analysis tools
│   ├── dashboard/      # Web dashboard
│   ├── monitoring/     # System monitoring
│   └── notebooks/      # Jupyter notebooks
├── logs/              # Log files
│   ├── backfill/      # Backfill operation logs
│   └── collector/     # Collector operation logs
├── migrations/        # Database migrations
│   ├── darkpool/     # Dark pool migrations
│   └── news/         # News migrations
├── scripts/          # Utility scripts
│   ├── db/          # Database utilities
│   ├── deployment/  # Deployment scripts
│   ├── maintenance/ # Maintenance scripts
│   ├── monitoring/  # Monitoring scripts
│   ├── utils/       # Utility functions
│   └── verification/# Data verification scripts
└── tests/           # Test files
```

## Features

- Dark pool trade collection and analysis
- News headline collection and sentiment analysis
- Options flow data collection and analysis
- Real-time data processing and monitoring
- Automated data validation and verification
- Web dashboard for data visualization
- Systemd service integration for production deployment

## Requirements

- Python 3.8+
- PostgreSQL 12+
- Redis (for Celery)
- Required Python packages (see requirements.txt)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/vvv-invest.git
cd vvv-invest
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
pip install -r requirements-test.txt  # For development
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Usage

### Running Collectors

1. Dark Pool Collector:
```bash
python -m collectors.darkpool.dark_pool_trades
```

2. News Collector:
```bash
python -m collectors.news.newscollector
```

3. Options Flow Collector:
```bash
python -m collectors.options.options_flow_collector
```

### Running with Celery

1. Start Redis server
2. Start Celery worker:
```bash
celery -A config.celery.celery_app worker --loglevel=info
```

3. Start Celery beat (for scheduled tasks):
```bash
celery -A config.celery.celery_app beat --loglevel=info
```

### Running the Dashboard

```bash
python -m flow_analysis.dashboard.app
```

## Development

- Follow PEP 8 style guide
- Write tests for new features
- Update documentation as needed
- Use pre-commit hooks for code quality

## Deployment

See `docs/deployment/DEPLOYMENT.md` for detailed deployment instructions.

## Environment Variables

Required environment variables:
- `UW_API_TOKEN`: API token for data collection
- `DB_HOST`: Database host
- `DB_PORT`: Database port
- `DB_NAME`: Database name
- `DB_USER`: Database user
- `DB_PASSWORD`: Database password

## License

MIT License - see LICENSE file for details
