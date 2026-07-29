# Документация проекта

Две «живые» поверхности, каждая обновляется автоматически при изменении — без
отдельного сервиса под документацию.

## 1. dbt-витрины + lineage → GitHub Pages

- **Источник правды:** `dbt/petbuddy/models/**/schema.yml` (описания моделей и
  колонок). Правишь модель — правишь её описание рядом.
- **Генерация:** таск `publish_docs` в DAG `rizottoaria__petbuddy_dbt_marts`
  запускает `dbt docs generate` (после сборки витрин, у него уже есть доступ к
  ClickHouse) и пушит `index.html` + `manifest.json` + `catalog.json` в ветку
  `gh-pages`. Обновляется каждый прогон DAG.
- **Хостинг:** GitHub Pages из ветки `gh-pages` →
  `https://rizottoaria.github.io/dags3.0/` (интерактивный граф lineage внизу
  справа, «View Lineage Graph»).

### Разовая настройка
1. **Токен для пуша.** Создать fine-grained PAT (Settings → Developer settings →
   Personal access tokens) с правом **Contents: Read and write** на репо
   `dags3.0`. В Airflow (Admin → Variables) завести переменную
   `GITHUB_DOCS_TOKEN` со значением токена. Без неё `publish_docs` мягко
   пропускается (пайплайн не падает).
2. **Включить Pages.** Settings → Pages → Source: *Deploy from a branch* →
   ветка `gh-pages`, папка `/ (root)`. (Если репо приватный — нужен платный план
   GitHub или сделать репо публичным.)

## 2. DAG-и → Airflow UI

Описание каждого DAG-а живёт в его docstring и прокинуто в UI через
`doc_md=__doc__`. Airflow сам перечитывает файлы — документация всегда актуальна,
отдельный хостинг не нужен. Открывается на странице DAG-а, вкладка с описанием.

## Гейт качества

`.github/workflows/dbt-docs-check.yml` на каждый PR/push, меняющий `dbt/**`,
запускает `scripts/ci/check_descriptions.py` — падает, если у обязательной
витрины (`marts/finance/mart_*`) нет `description`. Список обязательных моделей
расширяется в `REQUIRED_GLOBS` внутри скрипта по мере документирования.
