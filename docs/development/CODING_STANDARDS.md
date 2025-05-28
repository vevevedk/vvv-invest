# Coding Standards

This document outlines the coding standards and best practices for the VVV-Invest project.

## Python Code Style

### General Rules
1. Follow PEP 8 style guide
2. Use meaningful variable and function names
3. Keep functions small and focused
4. Use type hints for function arguments and return values
5. Document all public functions and classes

### Naming Conventions
```python
# Variables and functions (snake_case)
user_name = "John"
def calculate_total():
    pass

# Classes (CamelCase)
class DataCollector:
    pass

# Constants (UPPER_CASE)
MAX_RETRIES = 3
API_ENDPOINT = "https://api.example.com"

# Private members (single underscore prefix)
def _internal_helper():
    pass
```

### Imports
```python
# Standard library imports
import os
import sys
from datetime import datetime

# Third-party imports
import requests
import pandas as pd

# Local imports
from .utils import helper
from ..config import settings
```

### Docstrings
```python
def process_data(data: List[Dict], validate: bool = True) -> Dict:
    """Process the input data and return the result.

    Args:
        data (List[Dict]): List of data items to process
        validate (bool, optional): Whether to validate the data. Defaults to True.

    Returns:
        Dict: Processed data with validation results

    Raises:
        ValueError: If data is empty or invalid
    """
    pass
```

## Error Handling

### Exception Handling
```python
try:
    result = process_data(data)
except ValueError as e:
    logger.error(f"Invalid data: {str(e)}")
    raise
except Exception as e:
    logger.error(f"Unexpected error: {str(e)}")
    raise
```

### Custom Exceptions
```python
class CollectorError(Exception):
    """Base exception for collector errors."""
    pass

class APIError(CollectorError):
    """Exception raised for API-related errors."""
    pass
```

## Logging

### Configuration
```python
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
handler.setFormatter(formatter)
logger.addHandler(handler)
```

### Usage
```python
logger.debug("Detailed information for debugging")
logger.info("General information about program execution")
logger.warning("Warning messages for potentially problematic situations")
logger.error("Error messages for serious problems")
logger.critical("Critical messages for fatal errors")
```

## Testing

### Test Structure
```python
def test_process_data():
    # Arrange
    input_data = [{"id": 1, "value": 100}]
    
    # Act
    result = process_data(input_data)
    
    # Assert
    assert result["processed"] == 1
    assert result["valid"] == True
```

### Test Naming
- Test files: `test_*.py`
- Test functions: `test_*`
- Test classes: `Test*`

## Database

### SQL Style
```sql
-- Use uppercase for SQL keywords
SELECT 
    id,
    name,
    created_at
FROM 
    users
WHERE 
    status = 'active'
    AND created_at > '2025-01-01'
ORDER BY 
    created_at DESC;
```

### Query Organization
1. Use meaningful table and column names
2. Include appropriate indexes
3. Use transactions for multiple operations
4. Handle NULL values appropriately

## API Integration

### Request Handling
```python
def make_api_request(endpoint: str, params: Dict) -> Dict:
    """Make an API request with proper error handling and retries."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                endpoint,
                params=params,
                timeout=30,
                headers={"Authorization": f"Bearer {API_TOKEN}"}
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)}")
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
```

## Code Organization

### Project Structure
```
project/
├── collectors/
│   ├── __init__.py
│   ├── base_collector.py
│   └── dark_pool_collector.py
├── config/
│   ├── __init__.py
│   └── settings.py
├── tests/
│   ├── __init__.py
│   └── test_collectors.py
└── utils/
    ├── __init__.py
    └── helpers.py
```

### Module Organization
1. Imports (standard library, third-party, local)
2. Constants
3. Class definitions
4. Function definitions
5. Main execution block

## Version Control

### Git Commit Messages
```
feat: add new feature
fix: fix bug
docs: update documentation
style: format code
refactor: refactor code
test: add tests
chore: update dependencies
```

### Branch Naming
- Feature branches: `feature/feature-name`
- Bug fixes: `fix/bug-description`
- Hotfixes: `hotfix/issue-description`

## Performance

### Optimization Guidelines
1. Use appropriate data structures
2. Implement caching where beneficial
3. Optimize database queries
4. Use async/await for I/O operations
5. Profile code to identify bottlenecks

### Memory Management
1. Use context managers for resources
2. Implement proper cleanup in destructors
3. Monitor memory usage
4. Use generators for large datasets

## Security

### Best Practices
1. Never commit sensitive data
2. Use environment variables for secrets
3. Implement proper input validation
4. Use parameterized queries
5. Follow the principle of least privilege

## Documentation

### Code Comments
1. Explain why, not what
2. Keep comments up to date
3. Use TODO comments for future improvements
4. Document complex algorithms

### README Files
1. Project overview
2. Installation instructions
3. Usage examples
4. Configuration options
5. Troubleshooting guide 