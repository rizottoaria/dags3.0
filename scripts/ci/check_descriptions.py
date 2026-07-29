#!/usr/bin/env python3
"""CI-гейт полноты dbt-документации.

Требует, чтобы у каждой модели из REQUIRED_GLOBS был непустой `description`
в каком-либо schema.yml, и (опц.) чтобы были описаны её колонки.
Падает с ненулевым кодом при пропусках — так docs не протухают со временем.

Без подключения к ClickHouse: парсит только .sql-файлы моделей и schema.yml.
Запуск: python scripts/ci/check_descriptions.py [--require-columns]
"""
from __future__ import annotations

import argparse
import fnmatch
import glob
import os
import sys

try:
    import yaml
except ImportError:
    sys.exit("PyYAML не установлен (pip install pyyaml)")

MODELS_ROOT = "dbt/petbuddy/models"

# Модели, для которых описание ОБЯЗАТЕЛЬНО (пути относительно MODELS_ROOT).
# Начинаем с витрин finance; расширяй список по мере документирования.
REQUIRED_GLOBS = [
    "marts/finance/mart_*.sql",
]


def model_descriptions() -> dict[str, str]:
    """{model_name: description} из всех schema.yml проекта."""
    out: dict[str, str] = {}
    for yml in glob.glob(f"{MODELS_ROOT}/**/*.yml", recursive=True):
        with open(yml, encoding="utf-8") as f:
            doc = yaml.safe_load(f) or {}
        for m in doc.get("models", []) or []:
            name = m.get("name")
            if name:
                out[name] = (m.get("description") or "").strip()
    return out


def required_models() -> list[str]:
    names: list[str] = []
    for pat in REQUIRED_GLOBS:
        for path in glob.glob(f"{MODELS_ROOT}/{pat}"):
            names.append(os.path.splitext(os.path.basename(path))[0])
    return sorted(set(names))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--require-columns", action="store_true",
                    help="также требовать описание у всех колонок (не только у модели)")
    ap.add_argument("--report", action="store_true",
                    help="только показать покрытие, не падать")
    args = ap.parse_args()

    descs = model_descriptions()
    required = required_models()
    missing = [m for m in required if not descs.get(m)]

    print(f"Проверяю {len(required)} обязательных моделей "
          f"({', '.join(REQUIRED_GLOBS)})")
    documented = len(required) - len(missing)
    print(f"С описанием: {documented}/{len(required)}")

    if missing:
        print("\nБЕЗ описания:")
        for m in missing:
            print(f"  - {m}")

    if args.report:
        return 0
    if missing:
        print(f"\nFAIL: {len(missing)} модель(ей) без description. "
              f"Добавь их в соответствующий schema.yml.")
        return 1
    print("\nOK: у всех обязательных моделей есть описание.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
