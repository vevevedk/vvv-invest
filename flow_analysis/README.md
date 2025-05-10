# News Collector System

A robust system for collecting, processing, and analyzing financial news data.

## Features

- News headline collection from Unusual Whales API
- Real-time sentiment analysis and impact scoring
- Duplicate detection and data validation
- System health monitoring and metrics collection
- Comprehensive error handling and recovery
- Market hours awareness
- Database storage with connection pooling

## System Components

### 1. News Collector (`news_collector.py`)
- Main collection script
- Handles API requests and data processing
- Manages database operations
- Implements error recovery and retry logic

### 2. Monitoring (`monitoring.py`)
- System metrics collection
- Health checks
- Performance monitoring
- Resource usage tracking

### 3. Data Validation (`data_validation.py`)
- Data quality checks
- Input validation
- Cleaning and normalization
- Validation result storage

### 4. Testing
- Comprehensive test suite
- Coverage reporting
- Mocked API and database operations
- Integration tests

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

## Running Tests

1. Run the test suite with coverage:
```bash
python scripts/run_tests.py
```

2. View coverage reports:
- Console report is displayed after test completion
- HTML report is generated in `reports/coverage/html_<timestamp>/`

## Database Schema

### News Headlines Table
```sql
CREATE TABLE trading.news_headlines (
    id SERIAL PRIMARY KEY,
    headline TEXT NOT NULL,
    source VARCHAR(100),
    published_at TIMESTAMP WITH TIME ZONE,
    symbols TEXT[],
    sentiment FLOAT,
    impact_score INTEGER,
    collected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(headline, published_at)
);
```

### System Metrics Table
```sql
CREATE TABLE trading.system_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    cpu_usage FLOAT,
    memory_usage FLOAT,
    disk_usage FLOAT,
    api_latency FLOAT,
    db_latency FLOAT
);
```

### Health Checks Table
```sql
CREATE TABLE trading.health_checks (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_healthy BOOLEAN,
    errors TEXT[],
    warnings TEXT[]
);
```

### Validation Results Table
```sql
CREATE TABLE trading.validation_results (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    record_id INTEGER,
    is_valid BOOLEAN,
    errors TEXT[],
    warnings TEXT[],
    cleaned_data JSONB
);
```

## Error Handling

The system implements comprehensive error handling:

1. **API Requests**:
   - Exponential backoff retry
   - Rate limiting
   - Request timeout handling

2. **Database Operations**:
   - Connection pooling
   - Automatic reconnection
   - Transaction management
   - Deadlock handling

3. **Data Processing**:
   - Input validation
   - Data cleaning
   - Error logging
   - Recovery procedures

## Monitoring

The system includes:

1. **System Metrics**:
   - CPU usage
   - Memory usage
   - Disk usage
   - API latency
   - Database latency

2. **Health Checks**:
   - Database connectivity
   - API accessibility
   - Resource availability
   - System status

3. **Data Quality**:
   - Validation results
   - Error tracking
   - Performance metrics

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 