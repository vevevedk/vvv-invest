from typing import Dict, Any, List
from datetime import datetime
import pytz

class NewsSchemaValidator:
    """Validator for news headline data."""
    
    @staticmethod
    def validate(data: Dict[str, Any]) -> bool:
        """
        Validate a news headline entry.
        
        Args:
            data: Dictionary containing news headline data
            
        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = [
            'headline',
            'source',
            'created_at',
            'tickers',
            'is_major',
            'sentiment',
            'tags',
            'meta'
        ]
        
        # Check all required fields are present
        for field in required_fields:
            if field not in data:
                return False
        
        # Validate field types and values
        if not isinstance(data['headline'], str) or not data['headline'].strip():
            return False
        if not isinstance(data['source'], str) or not data['source'].strip():
            return False
        if not isinstance(data['created_at'], str):
            return False
        if not isinstance(data['tickers'], list) or not all(isinstance(s, str) for s in data['tickers']):
            return False
        if not isinstance(data['is_major'], bool):
            return False
        if not isinstance(data['sentiment'], str):
            return False
        if not isinstance(data['tags'], list) or not all(isinstance(t, str) for t in data['tags']):
            return False
        if not isinstance(data['meta'], dict):
            return False
        # Validate sentiment is one of the allowed values
        allowed_sentiments = {'positive', 'negative', 'neutral'}
        if data['sentiment'] not in allowed_sentiments:
            return False
        return True

class DarkPoolSchemaValidator:
    """Validator for dark pool trade data."""
    
    @staticmethod
    def validate(data: Dict[str, Any]) -> bool:
        """
        Validate a dark pool trade entry.
        
        Args:
            data: Dictionary containing dark pool trade data
            
        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = {
            'symbol': str,
            'price': float,
            'quantity': int,
            'executed_at': datetime,
            'venue': str,
            'trade_type': str,
            'meta': Dict[str, Any]
        }
        
        # Check all required fields are present
        for field, field_type in required_fields.items():
            if field not in data:
                return False
            if not isinstance(data[field], field_type):
                return False
        
        # Validate field values
        if not data['symbol'].strip():
            return False
            
        if data['price'] <= 0:
            return False
            
        if data['quantity'] <= 0:
            return False
            
        if not data['venue'].strip():
            return False
            
        if not data['trade_type'].strip():
            return False
            
        if not isinstance(data['meta'], dict):
            return False
        
        return True
