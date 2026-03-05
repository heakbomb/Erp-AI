import os
import math
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import MongoClient
from pyproj import Transformer
from sqlalchemy import create_engine, text


# -------------------------
# DEBUG
# -------------------------
print("### SCRIPT LOADED ###")


# -------------------------
# UTIL
# -------------------------
def as_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, (int, float)):
            return float(v)
        s = str(v).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def dist_m(ax: float, ay: float, bx: float, by: float) -> int:
    return int(round(math.sqrt((ax - bx) ** 2 + (ay - by) ** 2)))


# -------------------------
# MySQL
# -------------------------
def mysql_engine_from_env():
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT", "3306")
    db = os.getenv("MYSQL_DB")
    user = os.getenv("MYSQL_USER")
    pw = os.getenv("MYSQL_PASSWORD")

    if not host or not db or not user or pw is None:
        raise RuntimeError("MySQL env가 부족합니다. MYSQL_HOST/DB/USER/PASSWORD 확인")

    url = f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}?charset=utf8mb4"
    return create_engine(url, pool_pre_ping=True)


def read_store_gps(engine, store_id: int) -> Tuple[float, float, int]:
    sql = text("""
        SELECT latitude, longitude, gps_radius_m
        FROM store_gps
        WHERE store_id = :store_id
        LIMIT 1
    """)
    with engine.connect() as conn:
        row = conn.execute(sql, {"store_id": store_id}).mappings().first()

    if not row:
        raise RuntimeError(f"store_gps에 store_id={store_id} 좌표가 없습니다.")

    lat = as_float(row["latitude"])
    lng = as_float(row["longitude"])
    radius = int(row["gps_radius_m"] or 0)

    if lat is None or lng is None:
        raise RuntimeError(f"store_gps 좌표가 비어있거나 숫자가 아닙니다. store_id={store_id}")

    return lat, lng, radius


def upsert_store_trade_area(engine, store_id: int, areas: List[Dict[str, Any]], source_version: str):
    """
    ✅ topN 후보를 store_trade_area에 저장하는 upsert

    필요 테이블 컬럼(이 코드 기준):
      store_id, trdar_cd, trdar_cd_nm, rank_no, distance_m,
      sigungu_cd, trdar_se_cd, calculated_at, source_version

    유니크키 추천:
      UNIQUE (store_id, trdar_cd)  또는 UNIQUE (store_id, rank_no)
    """
    sql = text("""
        INSERT INTO store_trade_area
          (store_id, trdar_cd, trdar_cd_nm, rank_no, distance_m, sigungu_cd, trdar_se_cd, calculated_at, source_version)
        VALUES
          (:store_id, :trdar_cd, :trdar_cd_nm, :rank_no, :distance_m, :sigungu_cd, :trdar_se_cd, NOW(), :source_version)
        ON DUPLICATE KEY UPDATE
          trdar_cd_nm = VALUES(trdar_cd_nm),
          rank_no = VALUES(rank_no),
          distance_m = VALUES(distance_m),
          sigungu_cd = VALUES(sigungu_cd),
          trdar_se_cd = VALUES(trdar_se_cd),
          calculated_at = NOW(),
          source_version = VALUES(source_version)
    """)

    rows = []
    for i, a in enumerate(areas, 1):
        rows.append({
            "store_id": store_id,
            "trdar_cd": a["TRDAR_CD"],
            "trdar_cd_nm": a.get("TRDAR_CD_NM"),
            "rank_no": i,
            "distance_m": int(a["distance_m"]),
            "sigungu_cd": a.get("SIGNGU_CD"),
            "trdar_se_cd": a.get("TRDAR_SE_CD"),
            "source_version": source_version[:64],
        })

    with engine.begin() as conn:
        if rows:
            conn.execute(sql, rows)


# -------------------------
# Mongo (erp_ai)
# -------------------------
def pick_nearby_trdars_from_mongo(
    db,
    store_xy: Tuple[float, float],
    top_n: int,
    radius_m: Optional[int],
    sigungu_cd: Optional[str],
    trdar_se_cd: Optional[str],
) -> List[Dict[str, Any]]:
    """
    ✅ Mongo: erp_ai.seoul_trdar_area_raw 에서 주변 상권 후보 topN 추출
    필드 가정(서울 상권 영역 raw):
      TRDAR_CD, TRDAR_CD_NM, XCNTS_VALUE, YDNTS_VALUE, SIGNGU_CD, TRDAR_SE_CD
    """
    col = db["seoul_trdar_area_raw"]

    q: Dict[str, Any] = {}
    if sigungu_cd:
        q["SIGNGU_CD"] = sigungu_cd
    if trdar_se_cd:
        q["TRDAR_SE_CD"] = trdar_se_cd

    proj = {
        "_id": 0,
        "TRDAR_CD": 1,
        "TRDAR_CD_NM": 1,
        "XCNTS_VALUE": 1,
        "YDNTS_VALUE": 1,
        "SIGNGU_CD": 1,
        "TRDAR_SE_CD": 1,
    }

    sx, sy = store_xy
    cand: List[Dict[str, Any]] = []

    for r in col.find(q, proj):
        x = as_float(r.get("XCNTS_VALUE"))
        y = as_float(r.get("YDNTS_VALUE"))
        if x is None or y is None:
            continue

        d = dist_m(sx, sy, x, y)

        # ✅ 반경 옵션
        if radius_m and radius_m > 0 and d > radius_m:
            continue

        cand.append({
            "TRDAR_CD": str(r.get("TRDAR_CD")),
            "TRDAR_CD_NM": r.get("TRDAR_CD_NM"),
            "SIGNGU_CD": r.get("SIGNGU_CD"),
            "TRDAR_SE_CD": r.get("TRDAR_SE_CD"),
            "distance_m": d,
        })

    cand.sort(key=lambda a: a["distance_m"])
    return cand[:top_n] if top_n > 0 else cand


def filter_by_selngq_exists(
    db,
    trdar_codes: List[str],
    q_from: str,
    q_to: str,
    min_quarters: int,
) -> List[str]:
    """
    ✅ Mongo: erp_ai.seoul_trdar_sales_raw 에서 분기 매출 데이터 존재하는 상권만 남김
    필드 가정:
      TRDAR_CD, STDR_YYQU_CD, THSMON_SELNG_AMT
    """
    col = db["seoul_trdar_sales_raw"]

    pipeline = [
        {"$match": {
            "TRDAR_CD": {"$in": trdar_codes},
            "STDR_YYQU_CD": {"$gte": q_from, "$lte": q_to},
            "THSMON_SELNG_AMT": {"$exists": True},
        }},
        {"$group": {"_id": {"t": "$TRDAR_CD", "q": "$STDR_YYQU_CD"}, "n": {"$sum": 1}}},
        {"$group": {"_id": "$_id.t", "nQuarters": {"$sum": 1}}},
        {"$match": {"nQuarters": {"$gte": min_quarters}}},
        {"$project": {"_id": 0, "TRDAR_CD": "$_id"}},
    ]
    ok = list(col.aggregate(pipeline, allowDiskUse=True))
    ok_set = {x["TRDAR_CD"] for x in ok}
    return [c for c in trdar_codes if c in ok_set]


# -------------------------
# MAIN
# -------------------------
def main():
    load_dotenv()
    print("### MAIN ENTERED ###")

    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--store-ids", type=int, nargs="+", required=True,
                    help="예: --store-ids 101 102 103")
    args = ap.parse_args()
    store_ids = args.store_ids

    # env options
    top_n = int(os.getenv("NEARBY_TOP_N", "30"))
    sigungu_cd = os.getenv("SIGNGU_CD", "").strip() or None
    trdar_se_cd = os.getenv("TRDAR_SE_CD", "").strip() or None
    q_from = os.getenv("Q_FROM", "20231")
    q_to = os.getenv("Q_TO", "20252")
    min_quarters = int(os.getenv("MIN_QUARTERS", "1"))

    # ✅ 좌표계: seoul_trdar_area_raw의 X/Y가 어떤 TM인지 (보통 5181)
    relm_tm_epsg = os.getenv("RELM_TM_EPSG", "EPSG:5181").strip()

    # ✅ 반경 사용 여부: 1이면 store_gps.gps_radius_m 적용
    use_radius = os.getenv("USE_RADIUS", "0").strip() == "1"

    # MySQL (1회)
    engine = mysql_engine_from_env()

    # Transformer (1회)
    transformer = Transformer.from_crs("EPSG:4326", relm_tm_epsg, always_xy=True)

    # Mongo (1회)
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.getenv("MONGO_DB", "erp_ai")  # ✅ 기본값 erp_ai
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=10000)
    client.admin.command("ping")
    db = client[mongo_db]

    # ✅ 상권 영역 데이터 존재 확인
    relm_cnt = db["seoul_trdar_area_raw"].count_documents({})
    if relm_cnt == 0:
        raise RuntimeError(f"Mongo({mongo_db}) seoul_trdar_area_raw rows가 없습니다.")

    print(f"✅ MONGO_DB={mongo_db} seoul_trdar_area_raw rows={relm_cnt}")
    print(f"✅ RELM_TM_EPSG={relm_tm_epsg} top_n={top_n} use_radius={use_radius}")
    print(f"✅ store_ids={store_ids}")

    ok = 0
    fail = 0

    for store_id in store_ids:
        try:
            lat, lng, radius_m = read_store_gps(engine, store_id)
            sx, sy = transformer.transform(lng, lat)

            print(f"\n▶ store_id={store_id} lat/lng=({lat},{lng}) radius={radius_m}m")
            print(f"  store_xy=({sx:.2f},{sy:.2f})")

            near = pick_nearby_trdars_from_mongo(
                db=db,
                store_xy=(sx, sy),
                top_n=top_n,
                radius_m=radius_m if use_radius else None,
                sigungu_cd=sigungu_cd,
                trdar_se_cd=trdar_se_cd,
            )
            if not near:
                raise RuntimeError("주변 상권 후보가 0개입니다. (반경/필터 확인)")

            # ✅ 매출분기 데이터 존재 필터(선택)
            near_codes = [x["TRDAR_CD"] for x in near]
            ok_codes = filter_by_selngq_exists(db, near_codes, q_from=q_from, q_to=q_to, min_quarters=min_quarters)
            if ok_codes:
                ok_set = set(ok_codes)
                near = [x for x in near if x["TRDAR_CD"] in ok_set]

            source_version = f"nearby_v2_{relm_tm_epsg}_top{top_n}_{q_from}_{q_to}"
            upsert_store_trade_area(engine, store_id, near, source_version)

            print(f"✅ saved store_trade_area store_id={store_id} n={len(near)}")
            for x in near[:5]:
                print(f" - {x['TRDAR_CD']} {x.get('TRDAR_CD_NM')} d={x['distance_m']}m")

            ok += 1

        except Exception as e:
            print(f"❌ store_id={store_id} FAILED: {e}")
            fail += 1

    print(f"\n✅ DONE. success={ok} fail={fail}")


if __name__ == "__main__":
    main()
