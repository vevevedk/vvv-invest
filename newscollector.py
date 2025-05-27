def insert_data(self, headlines):
        """Insert headlines into the database"""
        try:
            with self.engine.connect() as conn:
                for headline in headlines:
                    data = {
                        'headline': headline['headline'],
                        'source': headline['source'],
                        'created_at': headline['created_at'],
                        'sentiment': headline['sentiment'],
                        'is_major': headline['is_major'],
                        'tickers': headline['tickers'],
                        'tags': headline['tags'],
                        'collected_at': datetime.now(timezone.utc)
                    }
                    conn.execute(text("""
                        INSERT INTO trading.news_headlines 
                        (headline, source, created_at, sentiment, is_major, tickers, tags, collected_at)
                        VALUES (:headline, :source, :created_at, :sentiment, :is_major, :tickers, :tags, :collected_at)
                    """), data)
                conn.commit()
                logger.info(f"Successfully inserted {len(headlines)} headlines")
        except Exception as e:
            logger.error(f"Error inserting headlines: {e}")
            raise 