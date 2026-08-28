"""
arpu_prophet_forecast.py

Считает Prophet-прогноз накопительного ARPU на install для версии 1.0.24
(сегменты country = ALL / US / PH) и пишет результат в таблицу
`petbuddy_clean.arpu_prophet_raw`, которую подмешивает dbt-модель mart_arpu_forecast.

Вход:  petbuddy_clean.mart_arpu_prophet_input (чистый fixed-cohort ряд + синтетическая дата ds).
Выход: petbuddy_clean.arpu_prophet_raw (cohort_version, country, day_since_install,
                                        prophet_cum_ad_arpu, prophet_cum_arpu) за дни 1..HORIZON.

Параметры Prophet: interval_width=0.8, без сезонностей (у накопительной кривой их нет), линейный тренд.

"""
import os
import pandas as pd
import sys


ORIGIN = pd.Timestamp("2024-01-01")   # синтетическое начало ряда (см. mart_arpu_prophet_input.ds)
HORIZON = 60                          # прогноз до D60
INTERVAL_WIDTH = 0.8


def _client():
    import clickhouse_connect
    return clickhouse_connect.get_client(
        host=os.environ.get("CH_HOST", "clickhouse"),
        port=int(os.environ.get("CH_PORT", "8123")),
        username=os.environ.get("CH_USER", "dbt"),
        password=os.environ["CH_PASSWORD"],
    )


def _forecast_series(df: pd.DataFrame, ycol: str) -> dict:
    """Fit Prophet на (ds, y) и вернуть {day_since_install: yhat} за дни 1..HORIZON."""
    import pandas as pd
    from prophet import Prophet
    d = df[["ds", ycol]].rename(columns={"ds": "ds", ycol: "y"}).dropna()
    if len(d) < 2:          # Prophet требует >= 2 точки
        return {}
    n_future = HORIZON - int((d["ds"].max() - ORIGIN).days)
    m = Prophet(
        interval_width=INTERVAL_WIDTH,
        yearly_seasonality=False,
        weekly_seasonality=False,
        daily_seasonality=False,
    )
    m.fit(d)
    future = m.make_future_dataframe(periods=max(n_future, 0), freq="D")
    fc = m.predict(future)
    fc["day"] = (pd.to_datetime(fc["ds"]) - ORIGIN).dt.days
    return {int(day): max(0.0, float(y)) for day, y in zip(fc["day"], fc["yhat"])}


def main() -> int:
    import pandas as pd
    client = _client()
    src = client.query_df(
        "SELECT cohort_version, country, day_since_install, ds, cum_ad_arpu, cum_arpu "
        "FROM petbuddy_clean.mart_arpu_prophet_input"
    )
    if src.empty:
        raise RuntimeError("mart_arpu_prophet_input пуст — сначала соберите dbt-входы")
    src["ds"] = pd.to_datetime(src["ds"])

    rows = []
    segs = []
    for (version, country), g in src.groupby(["cohort_version", "country"]):
        g = g.sort_values("day_since_install")
        ad = _forecast_series(g, "cum_ad_arpu")
        tot = _forecast_series(g, "cum_arpu")
        if not ad and not tot:          # мало точек — сегмент пропускаем (prophet_* -> NULL)
            continue
        for day in range(1, HORIZON + 1):
            rows.append([version, country, day, ad.get(day, 0.0), tot.get(day, 0.0)])
        segs.append(f"{version}/{country}")
    print(f"LOG === prophet: {len(rows)} строк, {len(segs)} сегментов: {sorted(segs)}")

    client.command(
        "CREATE TABLE IF NOT EXISTS petbuddy_clean.arpu_prophet_raw ("
        "cohort_version String, country String, day_since_install Int32, "
        "prophet_cum_ad_arpu Float64, prophet_cum_arpu Float64) "
        "ENGINE = MergeTree ORDER BY (cohort_version, country, day_since_install)"
    )
    client.command("TRUNCATE TABLE petbuddy_clean.arpu_prophet_raw")
    client.insert(
        "petbuddy_clean.arpu_prophet_raw",
        rows,
        column_names=["cohort_version", "country", "day_since_install",
                      "prophet_cum_ad_arpu", "prophet_cum_arpu"],
    )
    print("LOG === arpu_prophet_raw обновлена")
    return 0


if __name__ == "__main__":
    sys.exit(main())
