-- Migration: Remove url column from trading.news_headlines
ALTER TABLE trading.news_headlines DROP COLUMN IF EXISTS url; 