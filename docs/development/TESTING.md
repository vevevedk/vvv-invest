# Testing Guide

This guide outlines the testing strategy, tools, and best practices for the VVV-Invest project.

## Testing Strategy

### 1. Test Types

#### Unit Tests
- Test individual components in isolation
- Mock external dependencies
- Focus on business logic
- Fast execution

#### Integration Tests
- Test component interactions
- Use test database
- Test API endpoints
- Verify data flow

#### End-to-End Tests
- Test complete workflows
- Use staging environment
- Verify system behavior
- Test user interactions

### 2. Test Coverage

#### Required Coverage
- Minimum 80% code coverage
- 100% coverage for critical paths
- Test all error conditions
- Test edge cases

#### Coverage Reports
```bash
# Generate coverage report
pytest --cov=collectors --cov-report=html

# View coverage report
open htmlcov/index.html
```

## Testing Tools

### 1. pytest

#### Installation
```bash
pip install pytest pytest-cov pytest-mock
```

#### Configuration
```ini
# pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = --verbose --cov=collectors --cov-report=term-missing
```

### 2. Mocking

#### Using pytest-mock
```python
def test_collector_api_call(mocker):
    # Mock API response
    mock_response = mocker.Mock()
    mock_response.json.return_value = {"data": "test"}
    mocker.patch("requests.get", return_value=mock_response)
    
    # Test collector
    collector = DataCollector()
    result = collector.fetch_data()
    
    assert result == {"data": "test"}
```

### 3. Database Testing

#### Using Test Database
```python
@pytest.fixture
def test_db():
    # Create test database
    engine = create_engine("postgresql://test:test@localhost/test_db")
    Base.metadata.create_all(engine)
    
    # Return test session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    yield session
    
    # Cleanup
    session.close()
    Base.metadata.drop_all(engine)
```

## Test Organization

### 1. Directory Structure
```
tests/
├── __init__.py
├── conftest.py
├── unit/
│   ├── __init__.py
│   ├── test_collectors.py
│   └── test_validators.py
├── integration/
│   ├── __init__.py
│   ├── test_api.py
│   └── test_database.py
└── e2e/
    ├── __init__.py
    └── test_workflows.py
```

### 2. Test Files

#### Unit Tests
```python
# tests/unit/test_collectors.py
import pytest
from collectors.dark_pool_collector import DarkPoolCollector

def test_dark_pool_collector_initialization():
    collector = DarkPoolCollector()
    assert collector is not None
    assert collector.logger is not None

def test_dark_pool_collector_validation():
    collector = DarkPoolCollector()
    data = {"symbol": "AAPL", "price": 150.0}
    assert collector.validate_data(data) is True
```

#### Integration Tests
```python
# tests/integration/test_api.py
import pytest
from collectors.api_client import APIClient

def test_api_client_authentication():
    client = APIClient()
    assert client.is_authenticated() is True

def test_api_client_data_fetch():
    client = APIClient()
    data = client.fetch_data("AAPL")
    assert data is not None
    assert "symbol" in data
```

#### End-to-End Tests
```python
# tests/e2e/test_workflows.py
import pytest
from collectors.workflow import DataCollectionWorkflow

def test_complete_data_collection():
    workflow = DataCollectionWorkflow()
    result = workflow.run()
    assert result.success is True
    assert result.data_count > 0
```

## Best Practices

### 1. Test Design

#### Arrange-Act-Assert Pattern
```python
def test_process_data():
    # Arrange
    input_data = [{"id": 1, "value": 100}]
    processor = DataProcessor()
    
    # Act
    result = processor.process(input_data)
    
    # Assert
    assert result["processed"] == 1
    assert result["valid"] is True
```

#### Test Isolation
```python
@pytest.fixture
def clean_database():
    # Setup
    db = Database()
    db.clear()
    
    yield db
    
    # Teardown
    db.clear()
```

### 2. Test Data

#### Fixtures
```python
@pytest.fixture
def sample_trade_data():
    return {
        "symbol": "AAPL",
        "price": 150.0,
        "quantity": 100,
        "timestamp": "2025-05-27T10:00:00Z"
    }
```

#### Data Factories
```python
def create_trade_data(symbol="AAPL", price=150.0):
    return {
        "symbol": symbol,
        "price": price,
        "quantity": 100,
        "timestamp": datetime.utcnow().isoformat()
    }
```

### 3. Error Testing

#### Testing Exceptions
```python
def test_invalid_data_handling():
    with pytest.raises(ValueError) as exc_info:
        process_data(None)
    assert "Data cannot be None" in str(exc_info.value)
```

#### Testing Edge Cases
```python
def test_empty_data_handling():
    result = process_data([])
    assert result["processed"] == 0
    assert result["valid"] is True
```

## Continuous Integration

### 1. GitHub Actions

#### Test Workflow
```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.8'
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
          pip install -r requirements-test.txt
      - name: Run tests
        run: |
          pytest --cov=collectors --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

### 2. Test Reports

#### Coverage Reports
- HTML reports for local development
- XML reports for CI integration
- Terminal output for quick feedback

#### Test Results
- JUnit XML reports
- Test duration statistics
- Failure summaries

## Performance Testing

### 1. Load Testing

#### Using Locust
```python
from locust import HttpUser, task, between

class CollectorUser(HttpUser):
    wait_time = between(1, 5)
    
    @task
    def collect_data(self):
        self.client.get("/api/collect")
```

### 2. Benchmarking

#### Using pytest-benchmark
```python
def test_collector_performance(benchmark):
    collector = DataCollector()
    result = benchmark(collector.collect_data)
    assert result is not None
```

## Security Testing

### 1. API Security

#### Testing Authentication
```python
def test_api_authentication():
    client = APIClient()
    assert client.authenticate() is True
    assert client.token is not None
```

#### Testing Authorization
```python
def test_api_authorization():
    client = APIClient()
    with pytest.raises(UnauthorizedError):
        client.access_protected_resource()
```

### 2. Data Security

#### Testing Data Validation
```python
def test_data_validation():
    validator = DataValidator()
    assert validator.validate_sensitive_data("password123") is False
    assert validator.validate_sensitive_data("P@ssw0rd!") is True
```

## Maintenance

### 1. Test Maintenance

#### Regular Updates
- Update test data
- Review test coverage
- Update test dependencies
- Clean up old tests

#### Test Documentation
- Document test scenarios
- Update test documentation
- Maintain test README
- Document test setup

### 2. Test Environment

#### Environment Setup
```bash
# Create test environment
python -m venv venv-test
source venv-test/bin/activate
pip install -r requirements-test.txt
```

#### Database Setup
```bash
# Create test database
createdb vvv_invest_test
python scripts/setup_test_db.py
``` 