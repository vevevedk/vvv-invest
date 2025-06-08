CREATE UNIQUE INDEX IF NOT EXISTS unique_news_headline
ON trading.news_headlines (headline, source, created_at); 