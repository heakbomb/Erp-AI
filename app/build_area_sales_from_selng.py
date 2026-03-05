import os
from datetime import timezone, timedelta
from typing import Dict, List, Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

KST = timezone(timedelta(hours=9))


# -------------------------
# date utils
# -------------------------
def quarter_to_months(q: str) -> List[str]:
    # q = "20231" (YYYYQ)
    year = int(q[:4])
    qq = int(q[4:5])
    start_month = (qq - 1) * 3 + 1
    return [f"{year}-{str(start_month + i).zfill(2)}" for i in range(3)]


def ym_to_int(ym: str) -> int:
    y, m = ym.split("-")
    return int(y) * 100 + int(m)


def sort_months(months: List[str]) -> List[str]:
    return sorted(months, key=ym_to_int)


# -------------------------
# MySQL
# -------------------------
def load_mysql_engine() -> Engine:
    host = os.getenv("MYSQL_HOST", "localhost")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    db = os.getenv("MYSQL_DB", "ERP")
    user = os.getenv("MYSQL_USER", "root")
    pw = os.getenv("MYSQL_PASSWORD", "")
    url = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600, future=True)


def fetch_nearby_trdars_from_mysql(
    engine: Engine,
    store_id: int,
    top_n: int,
    table_name: str,
) -> List[str]:
    """
    store_id 기준으로 주변 상권코드 목록을 MySQL에서 가져온다.

    기대 컬럼:
      - store_id
      - trdar_cd
      - rank_no (있으면 rank_no 기준 정렬)
      - distance_m (rank_no가 없으면 distance_m 기준)
    """
    # rank_no 컬럼 존재 여부 확인
    with engine.connect() as conn:
        cols = conn.execute(
            text("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :t
            """),
            {"t": table_name},
        ).fetchall()
    colset = {r[0].lower() for r in cols}

    if "rank_no" in colset:
        order_by = "rank_no ASC"
    elif "distance_m" in colset:
        order_by = "distance_m ASC"
    else:
        order_by = "trdar_cd ASC"

    sql = text(f"""
        SELECT trdar_cd
        FROM {table_name}
        WHERE store_id = :store_id
        ORDER BY {order_by}
        LIMIT :top_n
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {"store_id": store_id, "top_n": top_n}).fetchall()

    return [str(r[0]) for r in rows if r and r[0] is not None]


# -------------------------
# Mongo (erp_ai)
# -------------------------
def fetch_quarter_avg_sales_from_mongo(
    mongo: MongoClient,
    db_name: str,
    sales_col: str,
    nearby_trdars: List[str],
    q_from: str,
    q_to: str,
) -> List[dict]:
    """
    seoul_trdar_sales_raw 에서
    (분기, 상권) 단위로 THSMON_SELNG_AMT를 업종 row들을 합산 -> 상권 총매출
    그 다음 분기별로 상권들 평균 -> 주변 평균 매출(분기)
    """
    col = mongo[db_name][sales_col]

    pipeline = [
        {
            "$match": {
                "TRDAR_CD": {"$in": nearby_trdars},
                "STDR_YYQU_CD": {"$gte": q_from, "$lte": q_to},
                "THSMON_SELNG_AMT": {"$exists": True},
            }
        },
        # (q, trdar) 상권별 총매출(업종 합)
        {
            "$group": {
                "_id": {"q": "$STDR_YYQU_CD", "t": "$TRDAR_CD"},
                "trdarSales": {"$sum": {"$toDouble": "$THSMON_SELNG_AMT"}},
            }
        },
        # 분기별 주변 평균(상권 평균)
        {
            "$group": {
                "_id": "$_id.q",
                "areaAvgSales": {"$avg": "$trdarSales"},
                "nTrdars": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "quarter": "$_id", "areaAvgSales": 1, "nTrdars": 1}},
    ]

    return list(col.aggregate(pipeline, allowDiskUse=True))


# -------------------------
# transform quarter -> monthly
# -------------------------
def quarter_sales_to_monthly(quarter_rows: List[dict], mode: str) -> Dict[str, int]:
    """
    mode:
      - copy   : 분기값을 월로 그대로 복사 (추천/MVP)
      - divide3: 분기값을 3으로 나눠 월로 배분
    """
    out: Dict[str, int] = {}
    for r in quarter_rows:
        q = r["quarter"]
        v = float(r["areaAvgSales"])
        months = quarter_to_months(q)

        if mode == "divide3":
            v_month = v / 3.0
            for m in months:
                out[m] = int(round(v_month))
        else:
            for m in months:
                out[m] = int(round(v))
    return out


# -------------------------
# MySQL upsert result
# -------------------------
def upsert_monthly_area_sales(
    engine: Engine,
    store_id: int,
    monthly: Dict[str, int],
    source_version: str,
):
    sql = text("""
        INSERT INTO trade_area_monthly_area_sales
          (store_id, month_ym, area_avg_sales, source_version, calculated_at)
        VALUES
          (:store_id, :month_ym, :area_avg_sales, :source_version, NOW())
        ON DUPLICATE KEY UPDATE
          area_avg_sales = VALUES(area_avg_sales),
          source_version = VALUES(source_version),
          calculated_at = NOW()
    """)
    rows = [
        {
            "store_id": store_id,
            "month_ym": m,
            "area_avg_sales": int(v),
            "source_version": source_version,
        }
        for m, v in monthly.items()
    ]
    with engine.begin() as conn:
        conn.execute(sql, rows)


# -------------------------
# MAIN
# -------------------------
def main():
    load_dotenv()

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--store-id", type=int, required=True)
    ap.add_argument("--top-n", type=int, default=int(os.getenv("NEARBY_TOP_N", "30")))
    ap.add_argument("--nearby-table", type=str, default=os.getenv("NEARBY_TABLE", "store_trade_area"))
    args = ap.parse_args()

    store_id = args.store_id
    top_n = args.top_n
    nearby_table = args.nearby_table

    # Params
    q_from = os.getenv("Q_FROM", "20231")
    q_to = os.getenv("Q_TO", "20252")
    mode = os.getenv("SELNG_TO_MONTH_MODE", "copy").strip().lower()

    # Mongo (너 구조 기준)
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "erp_ai")
    sales_col = os.getenv("MONGO_SALES_COL", "seoul_trdar_sales_raw")

    # Connect
    mongo = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    mongo.admin.command("ping")

    engine = load_mysql_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    # 0) store_id -> nearby trdar list (자동)
    nearby_trdars = fetch_nearby_trdars_from_mysql(engine, store_id, top_n=top_n, table_name=nearby_table)
    if not nearby_trdars:
        raise RuntimeError(
            f"MySQL에서 주변 상권코드를 못 가져왔습니다. "
            f"table={nearby_table}, store_id={store_id}. "
            f"먼저 nearby_trdar_pick_for_store.py로 후보를 저장했는지 확인하세요."
        )

    print(f"✅ store_id={store_id} nearby_trdars={len(nearby_trdars)} (top_n={top_n})")
    print("   sample:", nearby_trdars[:10])

    # 1) 분기별 주변 평균 매출 집계
    quarter_rows = fetch_quarter_avg_sales_from_mongo(
        mongo=mongo,
        db_name=mongo_db,
        sales_col=sales_col,
        nearby_trdars=nearby_trdars,
        q_from=q_from,
        q_to=q_to,
    )
    if not quarter_rows:
        raise RuntimeError("집계 결과가 0건입니다. (분기/상권코드/데이터 유무 확인)")

    print("✅ quarter_rows sample:", quarter_rows[:3])

    # 2) 월로 펼치기
    monthly = quarter_sales_to_monthly(quarter_rows, mode=mode)
    months_sorted = sort_months(list(monthly.keys()))
    print(f"✅ monthly months={len(months_sorted)} range={months_sorted[0]}~{months_sorted[-1]} mode={mode}")

    # 3) MySQL upsert
    source_version = f"selngq_{mode}_{q_from}_{q_to}_top{top_n}"
    upsert_monthly_area_sales(engine, store_id, monthly, source_version)
    print("🎉 DONE: trade_area_monthly_area_sales upsert complete")


if __name__ == "__main__":
    main()
