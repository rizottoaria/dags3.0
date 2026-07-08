from datetime import datetime
from airflow.sdk import dag, task
from petbuddy_common import drain

@dag(dag_id="rizottoaria__petbuddy_events_backfill", schedule=None,
     start_date=datetime(2026, 1, 1), catchup=False, max_active_runs=1,
     tags=["petbuddy", "kafka", "backfill"])
def backfill():
    @task
    def run():
        msg = drain(batch=20000, max_loops=100)  
        print(f"LOG === {msg}")
    run()
backfill()