#!/usr/bin/env python3

"""
Data Validation Module for News Collector
Handles data quality checks, validation, and cleaning
"""

import logging
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from typing import Dict, List, Optional, Tuple, Set
import json
from pathlib import Path
import pytz
from dataclasses import dataclass
import re
from collections import defaultdict
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool

# Constants
MIN_HEADLINE_LENGTH = 10
MAX_HEADLINE_LENGTH = 500
MIN_SENTIMENT_SCORE = -1.0
MAX_SENTIMENT_SCORE = 1.0
MIN_IMPACT_SCORE = -10
MAX_IMPACT_SCORE = 10
VALID_SOURCES = {
    'Bloomberg', 'Reuters', 'CNBC', 'WSJ', 'MarketWatch',
    'Seeking Alpha', 'Benzinga', 'The Street', 'Yahoo Finance'
}
BLACKLISTED_WORDS = {
    'spam', 'clickbait', 'scam', 'fake', 'hoax', 'prank'
}

@dataclass
class ValidationResult:
    """Data validation result"""
    is_valid: bool
    timestamp: datetime
    record_id: int
    errors: List[str]
    warnings: List[str]
    cleaned_data: Dict

class DataValidator:
    """Validates and cleans news data"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.logger = self._setup_logger()
        self.eastern = pytz.timezone('US/Eastern')
        self.engine = self._create_engine()
        self.duplicate_cache = set()
        self._load_duplicate_cache()
        
    def _setup_logger(self) -> logging.Logger:
        """Set up logging for data validation"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Add file handler
        file_handler = logging.FileHandler(log_dir / "validation.log")
        file_handler.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        logger.addHandler(file_handler)
        
        return logger

    def _create_engine(self):
        """Create SQLAlchemy engine with connection pooling"""
        return create_engine(
            f"postgresql://{self.db_config['user']}:{self.db_config['password']}@"
            f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}",
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )

    def _load_duplicate_cache(self) -> None:
        """Load recent headlines into duplicate cache"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT headline, published_at
                    FROM trading.news_headlines
                    WHERE collected_at >= NOW() - INTERVAL '24 hours'
                """))
                for row in result:
                    self.duplicate_cache.add((row.headline, row.published_at))
        except Exception as e:
            self.logger.error(f"Error loading duplicate cache: {str(e)}")

    def validate_news_data(self, data: Dict) -> ValidationResult:
        """Validate a single news record"""
        errors = []
        warnings = []
        cleaned_data = data.copy()
        
        # Validate headline
        if not self._validate_headline(data.get('headline', ''), errors, warnings):
            cleaned_data['headline'] = self._clean_headline(data.get('headline', ''))
        
        # Validate source
        if not self._validate_source(data.get('source', ''), errors, warnings):
            cleaned_data['source'] = self._clean_source(data.get('source', ''))
        
        # Validate published_at
        if not self._validate_timestamp(data.get('published_at'), errors):
            return ValidationResult(False, datetime.now(self.eastern), 0, errors, warnings, {})
        
        # Validate symbols
        if not self._validate_symbols(data.get('symbols', []), errors, warnings):
            cleaned_data['symbols'] = self._clean_symbols(data.get('symbols', []))
        
        # Validate sentiment
        if not self._validate_sentiment(data.get('sentiment'), errors):
            cleaned_data['sentiment'] = 0.0
        
        # Validate impact score
        if not self._validate_impact_score(data.get('impact_score'), errors):
            cleaned_data['impact_score'] = 1
        
        # Check for duplicates
        if self._is_duplicate(cleaned_data):
            errors.append("Duplicate headline detected")
            return ValidationResult(False, datetime.now(self.eastern), 0, errors, warnings, {})
        
        # Check for blacklisted words
        if self._contains_blacklisted_words(cleaned_data['headline']):
            errors.append("Headline contains blacklisted words")
            return ValidationResult(False, datetime.now(self.eastern), 0, errors, warnings, {})
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            timestamp=datetime.now(self.eastern),
            record_id=data.get('id', 0),
            errors=errors,
            warnings=warnings,
            cleaned_data=cleaned_data
        )

    def _validate_headline(self, headline: str, errors: List[str], warnings: List[str]) -> bool:
        """Validate headline format and content"""
        if not headline:
            errors.append("Headline is empty")
            return False
        
        if len(headline) < MIN_HEADLINE_LENGTH:
            errors.append(f"Headline too short (minimum {MIN_HEADLINE_LENGTH} characters)")
            return False
        
        if len(headline) > MAX_HEADLINE_LENGTH:
            warnings.append(f"Headline too long (maximum {MAX_HEADLINE_LENGTH} characters)")
            return False
        
        if not headline[0].isupper():
            warnings.append("Headline should start with uppercase letter")
            return False
        
        return True

    def _clean_headline(self, headline: str) -> str:
        """Clean and normalize headline"""
        # Remove extra whitespace
        headline = ' '.join(headline.split())
        
        # Capitalize first letter
        headline = headline[0].upper() + headline[1:]
        
        # Truncate if too long
        if len(headline) > MAX_HEADLINE_LENGTH:
            headline = headline[:MAX_HEADLINE_LENGTH-3] + '...'
        
        return headline

    def _validate_source(self, source: str, errors: List[str], warnings: List[str]) -> bool:
        """Validate news source"""
        if not source:
            errors.append("Source is empty")
            return False
        
        if source not in VALID_SOURCES:
            warnings.append(f"Unknown source: {source}")
            return False
        
        return True

    def _clean_source(self, source: str) -> str:
        """Clean and normalize source name"""
        # Remove extra whitespace
        source = ' '.join(source.split())
        
        # Try to match with known sources
        for valid_source in VALID_SOURCES:
            if source.lower() in valid_source.lower():
                return valid_source
        
        return source

    def _validate_timestamp(self, timestamp: str, errors: List[str]) -> bool:
        """Validate timestamp format and range"""
        try:
            # Handle ISO format timestamps
            if isinstance(timestamp, str):
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            else:
                dt = pd.to_datetime(timestamp)
            
            # Convert to Eastern time for comparison
            dt = dt.astimezone(self.eastern)
            
            # Check if timestamp is in the future
            if dt > datetime.now(self.eastern):
                errors.append("Timestamp is in the future")
                return False
                
            # Check if timestamp is too old (more than 7 days)
            if dt < datetime.now(self.eastern) - timedelta(days=7):
                errors.append("Timestamp is too old")
                return False
                
            return True
        except Exception as e:
            errors.append(f"Invalid timestamp format: {str(e)}")
            return False

    def _validate_symbols(self, symbols: List[str], errors: List[str], warnings: List[str]) -> bool:
        """Validate stock symbols"""
        if not symbols:
            warnings.append("No symbols provided")
            return True  # Allow news without symbols
        
        valid_symbols = set()
        for symbol in symbols:
            if not isinstance(symbol, str):
                warnings.append(f"Invalid symbol type: {type(symbol)}")
                continue
            
            # Clean and validate symbol
            clean_symbol = symbol.strip().upper()
            if not re.match(r'^[A-Z]{1,5}$', clean_symbol):
                warnings.append(f"Invalid symbol format: {symbol}")
                continue
            
            valid_symbols.add(clean_symbol)
        
        if not valid_symbols:
            warnings.append("No valid symbols found")
            return True  # Allow news without valid symbols
        
        return True  # Always return True, just log warnings

    def _clean_symbols(self, symbols: List[str]) -> List[str]:
        """Clean and normalize stock symbols"""
        return [symbol.strip().upper() for symbol in symbols if isinstance(symbol, str)]

    def _validate_sentiment(self, sentiment: float, errors: List[str]) -> bool:
        """Validate sentiment score"""
        if sentiment is None:
            errors.append("Sentiment score is missing")
            return False
        
        try:
            sentiment = float(sentiment)
            if not MIN_SENTIMENT_SCORE <= sentiment <= MAX_SENTIMENT_SCORE:
                errors.append(f"Sentiment score out of range: {sentiment}")
                return False
            return True
        except (TypeError, ValueError):
            errors.append("Invalid sentiment score format")
            return False

    def _validate_impact_score(self, impact_score: float, errors: List[str]) -> bool:
        """Validate impact score range"""
        try:
            score = float(impact_score)
            if score < MIN_IMPACT_SCORE or score > MAX_IMPACT_SCORE:
                errors.append(f"Impact score out of range: {score}")
                return False
            return True
        except (ValueError, TypeError):
            errors.append("Invalid impact score format")
            return False

    def _is_duplicate(self, data: Dict) -> bool:
        """Check if headline is a duplicate"""
        key = (data['headline'], data['published_at'])
        if key in self.duplicate_cache:
            return True
        self.duplicate_cache.add(key)
        return False

    def _contains_blacklisted_words(self, headline: str) -> bool:
        """Check if headline contains blacklisted words"""
        headline_lower = headline.lower()
        return any(word in headline_lower for word in BLACKLISTED_WORDS)

    def save_validation_result(self, result: ValidationResult) -> None:
        """Save validation result to database"""
        try:
            # Convert datetime to string
            timestamp_str = result.timestamp.isoformat()
            
            # Convert cleaned_data timestamps to strings
            cleaned_data = result.cleaned_data.copy()
            for key, value in cleaned_data.items():
                if isinstance(value, datetime):
                    cleaned_data[key] = value.isoformat()
            
            with self.engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO trading.validation_results (
                        timestamp, record_id, is_valid, errors, warnings, cleaned_data
                    ) VALUES (:timestamp, :record_id, :is_valid, :errors, :warnings, :cleaned_data)
                """), {
                    'timestamp': timestamp_str,
                    'record_id': result.record_id,
                    'is_valid': result.is_valid,
                    'errors': json.dumps(result.errors),
                    'warnings': json.dumps(result.warnings),
                    'cleaned_data': json.dumps(cleaned_data)
                })
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error saving validation result: {str(e)}")

    def get_validation_stats(self, hours: int = 24) -> Dict:
        """Get validation statistics for the specified time period"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) as valid_records,
                        COUNT(DISTINCT record_id) as unique_records,
                        COUNT(DISTINCT jsonb_array_elements_text(errors)) as unique_errors,
                        COUNT(DISTINCT jsonb_array_elements_text(warnings)) as unique_warnings
                    FROM trading.validation_results
                    WHERE timestamp >= NOW() - INTERVAL ':hours hours'
                """), {'hours': hours})
                
                row = result.fetchone()
                return {
                    'total_records': row.total_records,
                    'valid_records': row.valid_records,
                    'unique_records': row.unique_records,
                    'unique_errors': row.unique_errors,
                    'unique_warnings': row.unique_warnings,
                    'validation_rate': row.valid_records / row.total_records if row.total_records > 0 else 0
                }
        except Exception as e:
            self.logger.error(f"Error getting validation stats: {str(e)}")
            return {}

def create_validation_tables(db_config: Dict[str, str]) -> None:
    """Create necessary tables for data validation"""
    try:
        engine = create_engine(
            f"postgresql://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        
        with engine.connect() as conn:
            # Create validation results table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS trading.validation_results (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                    record_id INTEGER NOT NULL,
                    is_valid BOOLEAN NOT NULL,
                    errors JSONB NOT NULL,
                    warnings JSONB NOT NULL,
                    cleaned_data JSONB NOT NULL
                );
            """))
            
            # Create indexes
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_validation_timestamp 
                ON trading.validation_results (timestamp);
                
                CREATE INDEX IF NOT EXISTS idx_validation_record 
                ON trading.validation_results (record_id);
            """))
            
            conn.commit()
    except Exception as e:
        logging.error(f"Error creating validation tables: {str(e)}")
        raise 