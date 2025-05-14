from news_celery_app import app

if __name__ == "__main__":
    # Send a task to run the news collector
    result = app.send_task(
        'news_celery_app.run_news_collector_task',
        queue='news_collector'
    )
    print(f"Task sent with ID: {result.id}") 