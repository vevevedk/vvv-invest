# Collector Framework Development Guide

This guide explains the collector framework architecture and how to develop new collectors.

## Architecture Overview

The collector framework is designed to be modular and extensible, with a common base structure for all collectors:

```
collectors/
├── __init__.py
├── base_collector.py      # Base collector class
├── schema_validation/     # Schema validation utilities
│   ├── __init__.py
│   └── schema_validator.py
├── darkpool/             # Dark pool collector
│   ├── __init__.py
│   └── dark_pool_trades.py
├── news/                 # News collector
│   ├── __init__.py
│   └── newscollector.py
└── options/              # Options collector
    ├── __init__.py
    └── options_flow_collector.py
```

## Base Collector

All collectors inherit from the `BaseCollector` class, which provides common functionality:

```python
class BaseCollector:
    def __init__(self):
        self.logger = self._setup_logging()
        self.db_config = self._load_db_config()
        self.schema_validator = self._get_schema_validator()

    def _setup_logging(self):
        """Set up logging configuration."""
        pass

    def _load_db_config(self):
        """Load database configuration."""
        pass

    def _get_schema_validator(self):
        """Get appropriate schema validator."""
        pass

    def validate_schema(self):
        """Validate database schema."""
        pass

    def collect_data(self):
        """Collect data from source."""
        raise NotImplementedError

    def process_data(self, data):
        """Process collected data."""
        raise NotImplementedError

    def save_data(self, data):
        """Save processed data to database."""
        raise NotImplementedError

    def run(self):
        """Main execution flow."""
        pass
```

## Schema Validation

Each collector has its own schema validator that inherits from `SchemaValidator`:

```python
class SchemaValidator:
    def __init__(self, engine):
        self.engine = engine

    def validate_schema(self):
        """Validate database schema."""
        raise NotImplementedError

    def validate_data(self, data):
        """Validate data format."""
        raise NotImplementedError
```

## Creating a New Collector

1. Create a new directory in `collectors/` for your collector:
```bash
mkdir -p collectors/new_collector
touch collectors/new_collector/__init__.py
```

2. Create the collector class:
```python
# collectors/new_collector/new_collector.py
from collectors.base_collector import BaseCollector
from collectors.schema_validation import SchemaValidator

class NewCollectorSchemaValidator(SchemaValidator):
    def validate_schema(self):
        # Implement schema validation
        pass

    def validate_data(self, data):
        # Implement data validation
        pass

class NewCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.schema_validator = NewCollectorSchemaValidator(self.engine)

    def collect_data(self):
        # Implement data collection
        pass

    def process_data(self, data):
        # Implement data processing
        pass

    def save_data(self, data):
        # Implement data saving
        pass
```

3. Create database migrations:
```bash
mkdir -p migrations/new_collector
touch migrations/new_collector/001_initial_schema.sql
```

4. Add schema validation:
```sql
-- migrations/new_collector/001_initial_schema.sql
CREATE TABLE IF NOT EXISTS trading.new_collector_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_new_collector_data_symbol ON trading.new_collector_data(symbol);
CREATE INDEX idx_new_collector_data_timestamp ON trading.new_collector_data(timestamp);
```

5. Create systemd service:
```ini
# config/systemd/new-collector.service
[Unit]
Description=New Collector Service
After=network.target

[Service]
Type=simple
User=vvv-invest
Group=vvv-invest
WorkingDirectory=/home/vvv-invest/app
Environment=PYTHONPATH=/home/vvv-invest/app
ExecStart=/home/vvv-invest/app/venv/bin/python -m collectors.new_collector.new_collector
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Best Practices

1. **Error Handling**
   - Use try-except blocks for all external calls
   - Log errors with appropriate context
   - Implement retry logic for transient failures

2. **Logging**
   - Use structured logging
   - Include relevant context in log messages
   - Set appropriate log levels

3. **Data Validation**
   - Validate schema before operations
   - Validate data format and content
   - Handle missing or invalid data gracefully

4. **Performance**
   - Use connection pooling
   - Implement batch processing
   - Cache frequently accessed data

5. **Testing**
   - Write unit tests for all components
   - Include integration tests
   - Test error conditions

## Example Implementation

Here's a complete example of a new collector:

```python
# collectors/new_collector/new_collector.py
import logging
from datetime import datetime
from typing import List, Dict, Optional

from collectors.base_collector import BaseCollector
from collectors.schema_validation import SchemaValidator
from config.db_config import get_db_config

class NewCollectorSchemaValidator(SchemaValidator):
    def validate_schema(self):
        """Validate the new collector schema."""
        try:
            # Check if table exists
            result = self.engine.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'trading'
                    AND table_name = 'new_collector_data'
                );
            """).scalar()
            
            if not result:
                self.logger.error("Table new_collector_data does not exist")
                return False
                
            # Check required columns
            columns = self.engine.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'trading'
                AND table_name = 'new_collector_data';
            """).fetchall()
            
            required_columns = {'id', 'symbol', 'timestamp', 'data', 'created_at'}
            existing_columns = {col[0] for col in columns}
            
            if not required_columns.issubset(existing_columns):
                self.logger.error("Missing required columns")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Schema validation failed: {str(e)}")
            return False

    def validate_data(self, data: List[Dict]) -> bool:
        """Validate the data format."""
        try:
            for item in data:
                if not all(k in item for k in ['symbol', 'timestamp', 'data']):
                    return False
                if not isinstance(item['symbol'], str):
                    return False
                if not isinstance(item['timestamp'], datetime):
                    return False
                if not isinstance(item['data'], dict):
                    return False
            return True
        except Exception as e:
            self.logger.error(f"Data validation failed: {str(e)}")
            return False

class NewCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.schema_validator = NewCollectorSchemaValidator(self.engine)
        self.logger = logging.getLogger(__name__)

    def collect_data(self) -> Optional[List[Dict]]:
        """Collect data from source."""
        try:
            # Implement data collection logic
            return []
        except Exception as e:
            self.logger.error(f"Data collection failed: {str(e)}")
            return None

    def process_data(self, data: List[Dict]) -> Optional[List[Dict]]:
        """Process collected data."""
        try:
            # Implement data processing logic
            return data
        except Exception as e:
            self.logger.error(f"Data processing failed: {str(e)}")
            return None

    def save_data(self, data: List[Dict]) -> bool:
        """Save processed data to database."""
        try:
            if not data:
                return True

            # Save data to database
            self.engine.execute("""
                INSERT INTO trading.new_collector_data (symbol, timestamp, data)
                VALUES (%s, %s, %s)
            """, [(item['symbol'], item['timestamp'], item['data']) for item in data])
            
            return True
        except Exception as e:
            self.logger.error(f"Data saving failed: {str(e)}")
            return False

    def run(self):
        """Main execution flow."""
        try:
            # Validate schema
            if not self.validate_schema():
                self.logger.error("Schema validation failed")
                return False

            # Collect data
            data = self.collect_data()
            if data is None:
                return False

            # Process data
            processed_data = self.process_data(data)
            if processed_data is None:
                return False

            # Validate data
            if not self.schema_validator.validate_data(processed_data):
                self.logger.error("Data validation failed")
                return False

            # Save data
            if not self.save_data(processed_data):
                return False

            return True
        except Exception as e:
            self.logger.error(f"Collector run failed: {str(e)}")
            return False

def main():
    collector = NewCollector()
    collector.run()

if __name__ == "__main__":
    main()
```

## Testing

Create a test file for your collector:

```python
# tests/test_new_collector.py
import pytest
from unittest.mock import Mock, patch
from collectors.new_collector.new_collector import NewCollector, NewCollectorSchemaValidator

@pytest.fixture
def collector():
    return NewCollector()

@pytest.fixture
def validator():
    engine = Mock()
    return NewCollectorSchemaValidator(engine)

def test_schema_validation(validator):
    # Mock engine response
    validator.engine.execute.return_value.scalar.return_value = True
    validator.engine.execute.return_value.fetchall.return_value = [
        ('id',), ('symbol',), ('timestamp',), ('data',), ('created_at',)
    ]
    
    assert validator.validate_schema() is True

def test_data_validation(validator):
    data = [
        {
            'symbol': 'AAPL',
            'timestamp': datetime.now(),
            'data': {'price': 150.0}
        }
    ]
    
    assert validator.validate_data(data) is True

def test_collector_run(collector):
    with patch.object(collector, 'collect_data') as mock_collect:
        with patch.object(collector, 'process_data') as mock_process:
            with patch.object(collector, 'save_data') as mock_save:
                mock_collect.return_value = [{'symbol': 'AAPL'}]
                mock_process.return_value = [{'symbol': 'AAPL'}]
                mock_save.return_value = True
                
                assert collector.run() is True
```

## Deployment

1. Add your collector to `setup.py`:
```python
setup(
    name="vvv-invest",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        # ... existing requirements ...
    ],
    entry_points={
        'console_scripts': [
            'new-collector=collectors.new_collector.new_collector:main',
        ],
    },
)
```

2. Update the deployment documentation with your collector's requirements.

3. Add monitoring and logging configuration for your collector.

## Support

For questions or issues:
1. Check the documentation
2. Review the code examples
3. Contact the development team 