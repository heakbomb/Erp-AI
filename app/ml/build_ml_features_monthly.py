# app/build_ml_features_monthly.py
import os
import pandas as pd
from sqlalchemy import create_engine, text

def get_engine():
    mysql_url = os.getenv("MYSQL_URL")
    if not mysql_url:
        raise RuntimeError("MYSQL_URL env is required")
    return create_engine(mysql_url, pool_pre_ping=True)

def main():
    engine = get_engine()

    # ✅ ym 범위: 최근 6개월 정도 (원하면 직접 바꿔)
    # 여기선 sales_daily_summary 기준으로 월 집계
    sql = """
    SELECT
      ds.store_id,
      DATE_FORMAT(ds.summary_date, '%%Y-%%m') AS ym,
      SUM(ds.total_sales) AS sales_amount
    FROM sales_daily_summary ds
    GROUP BY ds.store_id, DATE_FORMAT(ds.summary_date, '%%Y-%%m')
    """

    df = pd.read_sql(sql, engine)
    if df.empty:
        print("[BUILD] no rows from sales_daily_summary")
        return

    # 기본값 채우기 (나머지 feature는 0으로)
    df["cogs_rate"] = 0.0
    df["labor_amount"] = 0.0
    df["labor_rate"] = 0.0
    df["menu_count"] = 0
    df["avg_menu_price"] = 0.0
    df["near_store_count"] = 0
    df["area_avg_sales"] = 0.0
    df["price_gap_rate"] = 0.0

    # ✅ (선택) 업종/구 정보 넣고 싶으면 store + store_trade_area 조인해서 채우기
    # store_trade_area에 sigungu_cd_nm 있음
    # 아래는 storeId별 sigungu/industry 맵핑
    map_sql = """
    SELECT
      s.store_id,
      s.industry,
      sta.sigungu_cd_nm
    FROM store s
    LEFT JOIN store_trade_area sta ON sta.store_id = s.store_id
    """
    map_df = pd.read_sql(map_sql, engine)
    df = df.merge(map_df, on="store_id", how="left")

    # ✅ profit_amount/profit_rate 계산 (단순)
    df["profit_amount"] = df["sales_amount"] - (df["sales_amount"] * df["cogs_rate"]) - df["labor_amount"]
    df["profit_rate"] = df.apply(lambda r: (r["profit_amount"] / r["sales_amount"]) if r["sales_amount"] > 0 else 0, axis=1)

    # ✅ upsert
    upsert = """
    INSERT INTO ml_store_month_features (
      store_id, ym,
      sigungu_cd_nm, industry,
      sales_amount, cogs_rate, labor_amount, labor_rate,
      menu_count, avg_menu_price,
      near_store_count, area_avg_sales, price_gap_rate,
      profit_amount, profit_rate
    )
    VALUES (
      :store_id, :ym,
      :sigungu_cd_nm, :industry,
      :sales_amount, :cogs_rate, :labor_amount, :labor_rate,
      :menu_count, :avg_menu_price,
      :near_store_count, :area_avg_sales, :price_gap_rate,
      :profit_amount, :profit_rate
    )
    ON DUPLICATE KEY UPDATE
      sigungu_cd_nm = VALUES(sigungu_cd_nm),
      industry = VALUES(industry),
      sales_amount = VALUES(sales_amount),
      cogs_rate = VALUES(cogs_rate),
      labor_amount = VALUES(labor_amount),
      labor_rate = VALUES(labor_rate),
      menu_count = VALUES(menu_count),
      avg_menu_price = VALUES(avg_menu_price),
      near_store_count = VALUES(near_store_count),
      area_avg_sales = VALUES(area_avg_sales),
      price_gap_rate = VALUES(price_gap_rate),
      profit_amount = VALUES(profit_amount),
      profit_rate = VALUES(profit_rate),
      updated_at = NOW(6)
    """

    with engine.begin() as conn:
        conn.execute(text(upsert), df.to_dict(orient="records"))

    print(f"[BUILD] upsert rows = {len(df)}")

if __name__ == "__main__":
    main()
