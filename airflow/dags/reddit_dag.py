from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import praw

def fetch_reddit_data():
    reddit = praw.Reddit(
        client_id="CRq15T0OCnr3sT_tXjMzlw",
        client_secret="KiD1dP_fZ9TNUEn8z2qMP5eJyaDOCw",
        user_agent="RedditPipeline/0.1 by u/Due_Goal_1091"
    )

    subreddit = reddit.subreddit("python")
    top_post = next(subreddit.top(limit=1))

    with open("/opt/airflow/data/top_post.txt", "w") as f:
        f.write(f"Title: {top_post.title}\nScore: {top_post.score}")

with DAG(
    dag_id="reddit_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="fetch_reddit_data",
        python_callable=fetch_reddit_data,
    )
