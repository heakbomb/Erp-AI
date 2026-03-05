import os
import hashlib
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


# ---------------------------
# Utilities
# ---------------------------

def sha256_hash_list(values: List[str]) -> str:
    normalized = sorted([v.strip() for v in values if v and v.strip()])
    payload = "|".join(normalized).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()

def quarter_to_months(q: str) -> List[str]:
    year = int(q[:4])
    qq = int(q[4:5])
    start_month = (qq - 1) * 3 + 1
    return [f"{year}-{str(start_month+i).zfill(2)}" for i in range(3)]

def month_range_from_quarters(q_from: str, q_to: str) -> Tuple[str, str]:
    return quarter_to_months(q_from)[0], quarter_to_months(q_to)[-1]

def ym_to_int(ym: str) -> int:
    y, m = ym.split("-")
    return int(y) * 100 + int(m)

def sort_months(months: List[str]) -> List[str]:
    return sorted(months, key=ym_to_int)

def take_last_n(months: List[str], n: int) -> List[str]:
    months_sorted = sort_months(months)
    return months_sorted[-n:] if len(months_sorted) > n else months_sorted


# ---------------------------
# Config
# ---------------------------

@dataclass
class AppConfig:
    mongo_uri: str
    mongo_db: str
    mongo_rows_col: str

    mysql_host: str
    mysql_port: int
    mysql_db: str
    mysql_user: str
    mysql_password: str

    my_daily_sales_table: str
    my_daily_date_col: str
    my_daily_value_col: str

    calc_version: str
    k_window_months: int

def load_config() -> AppConfig:
    load_dotenv()
    return AppConfig(
        mongo_uri=os.getenv("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.getenv("MONGO_DB", "erp_public_data"),
        mongo_rows_col=os.getenv("MONGO_ROWS_COL", "public_data_rows"),

        mysql_host=os.getenv("MYSQL_HOST", "localhost"),
        mysql_port=int(os.getenv("MYSQL_PORT", "3306")),
        mysql_db=os.getenv("MYSQL_DB", "ERP"),
        mysql_user=os.getenv("MYSQL_USER", "root"),
        mysql_password=os.getenv("MYSQL_PASSWORD", ""),

        my_daily_sales_table=os.getenv("MYSQL_MY_DAILY_SALES_TABLE", "sales_daily_summary"),
        my_daily_date_col=os.getenv("MYSQL_MY_DAILY_DATE_COL", "summary_date"),
        my_daily_value_col=os.getenv("MYSQL_MY_DAILY_VALUE_COL", "total_sales"),

        calc_version=os.getenv("BENCHMARK_CALC_VERSION", "v1"),
        k_window_months=int(os.getenv("K_WINDOW_MONTHS", "6")),
    )

def make_mysql_engine(cfg: AppConfig) -> Engine:
    url = (
        f"mysql+pymysql://{cfg.mysql_user}:{cfg.mysql_password}"
        f"@{cfg.mysql_host}:{cfg.mysql_port}/{cfg.mysql_db}"
        f"?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600, future=True)


# ---------------------------
# Mongo aggregation: quarter avg index
# ---------------------------

def fetch_quarter_avg_index(
    mongo: MongoClient,
    cfg: AppConfig,
    nearby_trdars: List[str],
    q_from: str,
    q_to: str,
) -> List[Dict]:
    col = mongo[cfg.mongo_db][cfg.mongo_rows_col]

    pipeline = [
        {
            "$match": {
                "source": "SEOUL",
                "dataset": "VwsmTrdarFlpopQq",
                "TRDAR_CD": {"$in": nearby_trdars},
                "STDR_YYQU_CD": {"$gte": q_from, "$lte": q_to},
                "TOT_FLPOP_CO": {"$exists": True},
            }
        },
        # (q,t)로 1개로 줄이기
        {
            "$group": {
                "_id": {"q": "$STDR_YYQU_CD", "t": "$TRDAR_CD"},
                "flpopAvg": {"$avg": {"$toDouble": "$TOT_FLPOP_CO"}},
            }
        },
        {"$project": {"_id": 0, "q": "$_id.q", "t": "$_id.t", "flpop": "$flpopAvg"}},

        # Stor lookup (같은 q,t)에서 업종별 STOR_CO 합)
        {
            "$lookup": {
                "from": cfg.mongo_rows_col,
                "let": {"q": "$q", "t": "$t"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$source", "SEOUL"]},
                                    {"$eq": ["$dataset", "VwsmTrdarStorQq"]},
                                    {"$eq": ["$STDR_YYQU_CD", "$$q"]},
                                    {"$eq": ["$TRDAR_CD", "$$t"]},
                                ]
                            },
                            "STOR_CO": {"$exists": True},
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "storSum": {"$sum": {"$toDouble": "$STOR_CO"}},
                        }
                    },
                    {"$project": {"_id": 0, "stor": "$storSum"}},
                ],
                "as": "storAgg",
            }
        },
        {"$unwind": "$storAgg"},

        # index = flpop / stor
        {
            "$addFields": {
                "index": {
                    "$cond": [
                        {"$gt": ["$storAgg.stor", 0]},
                        {"$divide": ["$flpop", "$storAgg.stor"]},
                        None,
                    ]
                }
            }
        },
        {"$match": {"index": {"$ne": None}}},

        # 분기 평균
        {
            "$group": {
                "_id": "$q",
                "areaAvgIndex": {"$avg": "$index"},
                "nTrdars": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "quarter": "$_id", "areaAvgIndex": 1, "nTrdars": 1}},
    ]

    return list(col.aggregate(pipeline, allowDiskUse=True))


# ---------------------------
# MySQL: daily -> monthly SUM
# ---------------------------

def fetch_my_monthly_sales_from_daily(
    engine: Engine,
    cfg: AppConfig,
    store_id: int,
    from_month: str,
    to_month: str,
) -> Dict[str, int]:
    sql = text(f"""
        SELECT
          DATE_FORMAT({cfg.my_daily_date_col}, '%Y-%m') AS month_ym,
          CAST(SUM({cfg.my_daily_value_col}) AS DECIMAL(18,2)) AS my_sales
        FROM {cfg.my_daily_sales_table}
        WHERE store_id = :store_id
          AND {cfg.my_daily_date_col} >= STR_TO_DATE(CONCAT(:from_m, '-01'), '%Y-%m-%d')
          AND {cfg.my_daily_date_col} <  DATE_ADD(STR_TO_DATE(CONCAT(:to_m, '-01'), '%Y-%m-%d'), INTERVAL 1 MONTH)
        GROUP BY DATE_FORMAT({cfg.my_daily_date_col}, '%Y-%m')
        ORDER BY month_ym ASC
    """)
    out: Dict[str, int] = {}
    with engine.connect() as conn:
        rows = conn.execute(sql, {"store_id": store_id, "from_m": from_month, "to_m": to_month}).fetchall()
        for r in rows:
            # total_sales가 DECIMAL(12,2) 이므로 원 단위로 쓰려면 int(round())
            out[str(r.month_ym)] = int(round(float(r.my_sales)))
    return out


# ---------------------------
# Build series + k
# ---------------------------

def build_monthly_index_series(quarter_rows: List[Dict]) -> Dict[str, float]:
    monthly: Dict[str, float] = {}
    for row in quarter_rows:
        q = row["quarter"]
        idx = float(row["areaAvgIndex"])
        for m in quarter_to_months(q):
            monthly[m] = idx
    return monthly

def compute_k_scale(
    my_sales: Dict[str, int],
    area_index_monthly: Dict[str, float],
    k_window_months: int,
) -> float:
    common = [m for m in my_sales.keys() if m in area_index_monthly]
    common = take_last_n(sort_months(common), k_window_months)
    if not common:
        raise RuntimeError("k_scale 계산 실패: my_sales와 area_index가 겹치는 month가 없습니다.")
    my_avg = sum(my_sales[m] for m in common) / len(common)
    idx_avg = sum(area_index_monthly[m] for m in common) / len(common)
    if idx_avg <= 0:
        raise RuntimeError("k_scale 계산 실패: area_index 평균이 0 이하입니다.")
    return float(my_avg / idx_avg)


# ---------------------------
# MySQL: job + config + upsert
# ---------------------------

def start_job(engine: Engine, store_id: int) -> int:
    sql = text("""
        INSERT INTO trade_area_benchmark_job (config_id, store_id, status, started_at)
        VALUES (NULL, :store_id, 'RUNNING', NOW())
    """)
    with engine.begin() as conn:
        res = conn.execute(sql, {"store_id": store_id})
        return int(res.lastrowid)

def finish_job(engine: Engine, job_id: int, status: str, error_message: Optional[str] = None):
    sql = text("""
        UPDATE trade_area_benchmark_job
        SET status = :status,
            error_message = :error_message,
            finished_at = NOW()
        WHERE job_id = :job_id
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"status": status, "error_message": error_message, "job_id": job_id})

def upsert_config_new_active(
    engine: Engine,
    store_id: int,
    q_from: str,
    q_to: str,
    trdar_set_hash: str,
    trdar_count: int,
    calc_version: str,
    k_window_months: int,
) -> int:
    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE trade_area_benchmark_config
                SET is_active = 0
                WHERE store_id = :store_id AND is_active = 1
            """),
            {"store_id": store_id},
        )
        res = conn.execute(
            text("""
                INSERT INTO trade_area_benchmark_config
                  (store_id, q_from, q_to, trdar_set_hash, trdar_count, calc_version, k_window_months, is_active, created_at, updated_at)
                VALUES
                  (:store_id, :q_from, :q_to, :hash, :cnt, :ver, :kwin, 1, NOW(), NOW())
            """),
            {
                "store_id": store_id,
                "q_from": q_from,
                "q_to": q_to,
                "hash": trdar_set_hash,
                "cnt": trdar_count,
                "ver": calc_version,
                "kwin": k_window_months,
            },
        )
        return int(res.lastrowid)

def bulk_upsert_monthly_benchmark(
    engine: Engine,
    config_id: int,
    store_id: int,
    rows: List[Dict],
):
    if not rows:
        return
    sql = text("""
        INSERT INTO trade_area_monthly_benchmark
          (config_id, store_id, month_ym, my_sales, area_avg_sales_est, area_avg_index, k_scale, calculated_at)
        VALUES
          (:config_id, :store_id, :month_ym, :my_sales, :area_avg_sales_est, :area_avg_index, :k_scale, NOW())
        ON DUPLICATE KEY UPDATE
          my_sales = VALUES(my_sales),
          area_avg_sales_est = VALUES(area_avg_sales_est),
          area_avg_index = VALUES(area_avg_index),
          k_scale = VALUES(k_scale),
          calculated_at = NOW()
    """)
    with engine.begin() as conn:
        conn.execute(
            sql,
            [
                {"config_id": config_id, "store_id": store_id, **r}
                for r in rows
            ],
        )


# ---------------------------
# Orchestration
# ---------------------------

def run_benchmark_build(store_id: int, q_from: str, q_to: str, nearby_trdars: List[str]):
    cfg = load_config()
    engine = make_mysql_engine(cfg)

    # quick connection check
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))

    mongo = MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=5000)
    mongo.admin.command("ping")

    from_month, to_month = month_range_from_quarters(q_from, q_to)

    job_id = start_job(engine, store_id)
    print(f"✅ JOB START job_id={job_id} store_id={store_id} q={q_from}~{q_to} months={from_month}~{to_month}")

    try:
        # 1) My monthly sales (from daily)
        my_sales = fetch_my_monthly_sales_from_daily(engine, cfg, store_id, from_month, to_month)
        if not my_sales:
            raise RuntimeError("내 매출 데이터가 없습니다. sales_daily_summary에서 월 합계가 0건입니다.")

        # 2) Quarter avg index from Mongo
        quarter_rows = fetch_quarter_avg_index(mongo, cfg, nearby_trdars, q_from, q_to)
        if not quarter_rows:
            raise RuntimeError("Mongo에서 분기 지수(areaAvgIndex)를 가져오지 못했습니다. (0건)")

        # 3) Quarter -> month index series
        area_index_monthly = build_monthly_index_series(quarter_rows)

        # 4) k scale
        k_scale = compute_k_scale(my_sales, area_index_monthly, cfg.k_window_months)
        print(f"✅ k_scale={k_scale:.6f} (window={cfg.k_window_months} months)")

        # 5) Build rows (month list from index range)
        months = [m for m in area_index_monthly.keys() if from_month <= m <= to_month]
        months = sort_months(months)

        rows_to_save: List[Dict] = []
        for m in months:
            idx = float(area_index_monthly[m])
            my = int(my_sales.get(m, 0))
            est = int(round(idx * k_scale))
            rows_to_save.append({
                "month_ym": m,
                "my_sales": my,
                "area_avg_sales_est": est,
                "area_avg_index": idx,
                "k_scale": float(k_scale),
            })

        # 6) save config
        trdar_hash = sha256_hash_list(nearby_trdars)
        config_id = upsert_config_new_active(
            engine, store_id, q_from, q_to, trdar_hash, len(nearby_trdars), cfg.calc_version, cfg.k_window_months
        )
        print(f"✅ config saved config_id={config_id}")

        # 7) upsert monthly benchmark
        bulk_upsert_monthly_benchmark(engine, config_id, store_id, rows_to_save)
        print(f"✅ benchmark upserted rows={len(rows_to_save)}")

        finish_job(engine, job_id, "DONE", None)
        print("🎉 DONE")

    except Exception as e:
        finish_job(engine, job_id, "FAILED", str(e)[:500])
        raise


if __name__ == "__main__":
    load_dotenv()
    store_id = int(os.getenv("TARGET_STORE_ID", "11"))
    q_from = os.getenv("Q_FROM", "20241")
    q_to = os.getenv("Q_TO", "20252")
    nearby_trdars = [x.strip() for x in os.getenv("NEARBY_TRDARS", "").split(",") if x.strip()]
    if not nearby_trdars:
        raise RuntimeError("NEARBY_TRDARS가 비어있습니다. 예: 3110001,3110375,3110436")

    run_benchmark_build(store_id, q_from, q_to, nearby_trdars)
