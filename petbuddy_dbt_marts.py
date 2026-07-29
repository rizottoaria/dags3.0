"""
rizottoaria__petbuddy_dbt_marts

Ежедневная пересборка dbt-витрин в схеме petbuddy_clean (ClickHouse):
staging -> intermediate -> marts. Запускает `dbt build` (run + тесты качества)
через изолированный venv образа (/home/airflow/dbt-venv) — у dbt-core свои
пины зависимостей, конфликтующие с Airflow, поэтому он вынесен в отдельный venv.

Проект деплоится вместе с DAG'ами в /opt/airflow/dags/dbt/petbuddy.
Креды ClickHouse берутся из Airflow Variables (CH_DBT_*), не хардкодятся.
target/logs пишем в /tmp, чтобы не трогать примонтированную папку dags.
"""
from datetime import datetime, timedelta

from airflow.sdk import dag, task

DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
PROJECT_DIR = "/opt/airflow/dags/dbt/petbuddy"
TARGET_DIR = "/tmp/dbt_petbuddy/target"
LOG_DIR = "/tmp/dbt_petbuddy/logs"


@dag(
    dag_id="rizottoaria__petbuddy_dbt_marts",
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "clickhouse", "petbuddy", "marts"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    doc_md=__doc__,
)
def petbuddy_dbt_marts():

    @task(execution_timeout=timedelta(minutes=30))
    def dbt_build() -> None:
        import os
        import subprocess

        from airflow.sdk import Variable

        env = os.environ.copy()
        env["CH_HOST"] = Variable.get("CH_DBT_HOST", default="clickhouse")
        env["CH_USER"] = Variable.get("CH_DBT_USER", default="dbt")
        env["CH_PASSWORD"] = Variable.get("CH_DBT_PASSWORD")  # обязателен
        env["DBT_TARGET_PATH"] = TARGET_DIR
        env["DBT_LOG_PATH"] = LOG_DIR

        cmd = [
            DBT_BIN, "build",
            "--project-dir", PROJECT_DIR,
            "--profiles-dir", PROJECT_DIR,
            "--target", "dev",
        ]
        print("LOG === running:", " ".join(cmd))
        result = subprocess.run(cmd, capture_output=True, text=True, env=env)
        print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode != 0:
            raise RuntimeError(f"LOG === dbt build failed (exit={result.returncode})")
        print("LOG === dbt build passed")

    @task(execution_timeout=timedelta(minutes=15))
    def publish_docs() -> None:
        """Генерит dbt docs (index/manifest/catalog) и пушит в ветку gh-pages.

        gh-pages отдаётся GitHub Pages -> постоянный URL с lineage, обновляется
        каждый прогон. Токен берём из Airflow Variable GITHUB_DOCS_TOKEN
        (fine-grained PAT с правом Contents:write на репо dags3.0). Если токена
        нет — шаг мягко пропускается, пайплайн не падает.
        """
        import os
        import shutil
        import subprocess
        import tempfile

        from airflow.sdk import Variable

        token = Variable.get("GITHUB_DOCS_TOKEN", default=None)
        if not token:
            print("LOG === GITHUB_DOCS_TOKEN не задана — публикацию docs пропускаю")
            return

        env = os.environ.copy()
        env["CH_HOST"] = Variable.get("CH_DBT_HOST", default="clickhouse")
        env["CH_USER"] = Variable.get("CH_DBT_USER", default="dbt")
        env["CH_PASSWORD"] = Variable.get("CH_DBT_PASSWORD")
        env["DBT_TARGET_PATH"] = TARGET_DIR
        env["DBT_LOG_PATH"] = LOG_DIR

        gen = subprocess.run(
            [DBT_BIN, "docs", "generate",
             "--project-dir", PROJECT_DIR, "--profiles-dir", PROJECT_DIR,
             "--target", "dev"],
            capture_output=True, text=True, env=env,
        )
        print(gen.stdout[-2000:])
        if gen.returncode != 0:
            print(gen.stderr)
            raise RuntimeError("LOG === dbt docs generate failed")

        def git(args: list[str]) -> None:
            # токен никогда не печатаем: в сообщении только имя подкоманды,
            # а stderr прогоняем через redaction
            r = subprocess.run(["git", *args], capture_output=True, text=True, env=env)
            if r.returncode != 0:
                msg = (r.stderr or r.stdout or "").replace(token, "***")
                raise RuntimeError(f"LOG === git {args[0]} failed: {msg[:600]}")

        remote = f"https://x-access-token:{token}@github.com/rizottoaria/dags3.0.git"
        workdir = tempfile.mkdtemp(prefix="ghpages_")
        try:
            git(["clone", "--depth", "1", "--branch", "gh-pages", remote, workdir])
            for fn in ("index.html", "manifest.json", "catalog.json"):
                shutil.copy(os.path.join(TARGET_DIR, fn), os.path.join(workdir, fn))
            open(os.path.join(workdir, ".nojekyll"), "a").close()
            git(["-C", workdir, "add", "-A"])
            unchanged = subprocess.run(
                ["git", "-C", workdir, "diff", "--cached", "--quiet"]
            ).returncode == 0
            if unchanged:
                print("LOG === docs без изменений — коммит не нужен")
                return
            git(["-C", workdir,
                 "-c", "user.email=airflow@petbuddy", "-c", "user.name=airflow-docs",
                 "commit", "-m", "docs: refresh dbt docs (auto)"])
            git(["-C", workdir, "push", "origin", "gh-pages"])
            print("LOG === dbt docs опубликованы на gh-pages")
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

    dbt_build() >> publish_docs()


petbuddy_dbt_marts()
