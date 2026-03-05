import os
import pandas as pd
from sqlalchemy import create_engine, text

_ENGINE = None

def get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    mysql_url = os.getenv("MYSQL_URL")
    if not mysql_url:
        raise RuntimeError("MYSQL_URL env is required")

    # pool_pre_ping: 끊긴 커넥션 자동 감지(개발중 매우 유용)
    _ENGINE = create_engine(mysql_url, pool_pre_ping=True)
    return _ENGINE


def fetch_features(store_id: int, ym: str) -> pd.DataFrame:
    sql = text("""
      SELECT
        sigungu_cd_nm, industry,
        sales_amount, labor_amount, labor_rate,
        menu_count, avg_menu_price,
        price_gap_rate, near_store_count, area_avg_sales,
        cogs_rate, profit_amount, profit_rate
      FROM ml_store_month_features
      WHERE store_id = :store_id
        AND ym = :ym
      LIMIT 1
    """)

    df = pd.read_sql(sql, get_engine(), params={"store_id": store_id, "ym": ym})

    # 모델 입력 컬럼만 정리(혹시 None/NaN 처리)
    if not df.empty:
        df = df.fillna(0)

    return df
