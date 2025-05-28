"""
Unified schema validation module for all collectors.
This module provides functions to validate database schemas and data for all collectors.
"""

import logging
from typing import Dict, List, Optional
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
import pandas as pd

logger = logging.getLogger(__name__)

class SchemaValidator:
    """Base class for schema validation."""
    
    def __init__(self, engine: Engine):
        self.engine = engine
        self.inspector = inspect(engine)
    
    def validate_schema(self, schema_name: str, table_name: str) -> bool:
        """Validate if a table exists and has the correct schema."""
        try:
            if not self.inspector.has_schema(schema_name):
                logger.error(f"Schema {schema_name} does not exist")
                return False
                
            if not self.inspector.has_table(table_name, schema=schema_name):
                logger.error(f"Table {schema_name}.{table_name} does not exist")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error validating schema: {str(e)}")
            return False
    
    def get_table_columns(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get column information for a table."""
        try:
            return self.inspector.get_columns(table_name, schema=schema_name)
        except Exception as e:
            logger.error(f"Error getting table columns: {str(e)}")
            return []

class DarkPoolSchemaValidator(SchemaValidator):
    """Schema validator for dark pool trades."""
    
    REQUIRED_FIELDS = {
        'tracking_id': int,
        'symbol': str,
        'price': (int, float),
        'size': int,
        'executed_at': str
    }
    
    OPTIONAL_FIELDS = {
        'volume': (int, float),
        'premium': (int, float),
        'nbbo_ask': (int, float),
        'nbbo_bid': (int, float),
        'nbbo_ask_quantity': int,
        'nbbo_bid_quantity': int,
        'market_center': str,
        'sale_cond_codes': str,
        'ext_hour_sold_codes': str,
        'trade_code': str,
        'trade_settlement': str,
        'canceled': bool
    }
    
    def validate_trade(self, trade: dict) -> bool:
        """Validate a single dark pool trade."""
        try:
            # Check required fields
            for field, field_type in self.REQUIRED_FIELDS.items():
                if field not in trade:
                    logger.warning(f"Missing required field: {field}")
                    return False
                    
                if not isinstance(trade[field], field_type):
                    logger.warning(f"Invalid type for {field}: expected {field_type}, got {type(trade[field])}")
                    return False
            
            # Check optional fields if present
            for field, field_type in self.OPTIONAL_FIELDS.items():
                if field in trade and trade[field] is not None:
                    if not isinstance(trade[field], field_type):
                        logger.warning(f"Invalid type for {field}: expected {field_type}, got {type(trade[field])}")
                        return False
            
            # Validate timestamp format
            try:
                pd.to_datetime(trade['executed_at'])
            except Exception as e:
                logger.warning(f"Invalid timestamp format for executed_at: {str(e)}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating trade: {str(e)}")
            return False

class NewsSchemaValidator(SchemaValidator):
    """Schema validator for news headlines."""
    
    REQUIRED_COLUMNS = {
        'id': 'INTEGER',
        'headline': 'TEXT',
        'source': 'VARCHAR',
        'published_at': 'TIMESTAMP',
        'symbols': 'ARRAY',
        'sentiment': 'DECIMAL',
        'impact_score': 'INTEGER',
        'collected_at': 'TIMESTAMP'
    }
    
    def validate_news_schema(self) -> bool:
        """Validate the news headlines schema."""
        if not self.validate_schema('trading', 'news_headlines'):
            return False
            
        columns = self.get_table_columns('trading', 'news_headlines')
        column_names = {col['name']: col['type'].__class__.__name__ for col in columns}
        
        for col_name, col_type in self.REQUIRED_COLUMNS.items():
            if col_name not in column_names:
                logger.error(f"Missing required column: {col_name}")
                return False
                
        return True

def get_schema_validator(engine: Engine, collector_type: str) -> Optional[SchemaValidator]:
    """Factory function to get the appropriate schema validator."""
    validators = {
        'darkpool': DarkPoolSchemaValidator,
        'news': NewsSchemaValidator
    }
    
    validator_class = validators.get(collector_type)
    if validator_class:
        return validator_class(engine)
    return None 