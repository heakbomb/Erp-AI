# app/ml_features.py
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_ENGINE: Engine | None = None

def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        mysql_url = os.getenv("MYSQL_URL")
        if not mysql_url:
            raise RuntimeError("MYSQL_URL env is required")
        _ENGINE = create_engine(mysql_url, pool_pre_ping=True)
    return _ENGINE


def fetch_features(store_id: int, ym: str) -> pd.DataFrame:
    # ✅ MySQL 호환: %s placeholder 사용
    sql = """
      SELECT
        store_id, ym,
        sales_amount, cogs_rate, labor_amount, labor_rate,
        menu_count, avg_menu_price,
        near_store_count, area_avg_sales, price_gap_rate
      FROM ml_store_month_features
      WHERE store_id = %s
        AND ym = %s
      LIMIT 1
    """
    df = pd.read_sql(sql, get_engine(), params=(store_id, ym))
    return df
