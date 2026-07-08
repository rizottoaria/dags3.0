from datetime import datetime, timedelta
from airflow.sdk import dag, task
from petbuddy_common import drain

@dag(dag_id="rizottoaria__petbuddy_events_ongoing", schedule=timedelta(minutes=1),
     start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     default_args={"retries": 2, "retry_delay": timedelta(seconds=30)},
     tags=["petbuddy", "kafka", "ongoing"])
def ongoing():
    @task
    def run():
        msg = drain(batch=5000, max_loops=20)     # хвост новых событий
        print(f"LOG === {msg}")
    run()
ongoing()