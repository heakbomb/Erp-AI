import os
import math
from datetime import datetime, timezone

import numpy as np
from pymongo import MongoClient
from sqlalchemy import create_engine, text
from pyproj import Transformer

# ✅ .env 자동 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# Config
# =========================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "erp_ai")
MONGO_COLL = os.getenv("MONGO_COLL", "seoul_trdar_area_raw")  # ✅ raw로 변경(너 DB 기준)

MYSQL_URL = os.getenv("MYSQL_URL")
if not MYSQL_URL:
    raise RuntimeError("MYSQL_URL env is required")

# ✅ 최대 허용 거리 (상권 중심점 기준)
# - 더미/시연: 3~5km 권장
MAX_DISTANCE_M = int(os.getenv("MAX_DISTANCE_M", "3000"))

# 좌표계: WGS84(lat/lng) -> EPSG:5186 (Korea 2000 / Central Belt 2010)
# XCNTS/YDNTS 값 범위(약 200k/450k)와 잘 맞는 좌표계
WGS84_TO_TM = Transformer.from_crs(
    "EPSG:4326",
    "EPSG:5181",
    always_xy=True
)


# =========================Transformer.from_crs("EPSG:4326", "EPSG:5179", always_xy=True)
# Mongo load
# =========================
def load_trade_areas():
    client = MongoClient(MONGO_URI)
    coll = client[MONGO_DB][MONGO_COLL]

    cursor = coll.find(
        {},
        {
            "_id": 0,
            "TRDAR_CD": 1,
            "TRDAR_CD_NM": 1,
            "SIGNGU_CD_NM": 1,
            "XCNTS_VALUE": 1,
            "YDNTS_VALUE": 1,
        },
    )

    areas = []
    skipped = 0

    for d in cursor:
        try:
            trdar_cd = str(d.get("TRDAR_CD", "")).strip()
            if not trdar_cd:
                raise ValueError("TRDAR_CD missing")

            x = float(d["XCNTS_VALUE"])
            y = float(d["YDNTS_VALUE"])

            areas.append(
                (
                    trdar_cd,
                    str(d.get("TRDAR_CD_NM", "")).strip(),
                    str(d.get("SIGNGU_CD_NM", "")).strip(),
                    x,
                    y,
                )
            )
        except Exception as e:
            skipped += 1
            # 처음 몇 개만 로그
            if skipped <= 3:
                print("[WARN] skipped doc keys:", list(d.keys()))
                print("[WARN] reason:", e)
            continue

    if not areas:
        raise RuntimeError(f"No trade areas loaded from Mongo ({MONGO_DB}.{MONGO_COLL})")

    codes = np.array([a[0] for a in areas], dtype=object)
    names = np.array([a[1] for a in areas], dtype=object)
    sigungus = np.array([a[2] for a in areas], dtype=object)
    xs = np.array([a[3] for a in areas], dtype=np.float64)
    ys = np.array([a[4] for a in areas], dtype=np.float64)

    return codes, names, sigungus, xs, ys


# =========================
# MySQL load
# =========================
def load_stores(engine):
    sql = text("""
        SELECT store_id, latitude, longitude
        FROM store_gps
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    stores = []
    for r in rows:
        stores.append((int(r[0]), float(r[1]), float(r[2])))
    return stores


# =========================
# Nearest match
# =========================
def nearest_trade_area(lat, lng, codes, names, sigungus, xs, ys):
    # 위경도 -> TM meters
    x, y = WGS84_TO_TM.transform(lng, lat)

    dx = xs - x
    dy = ys - y
    dist2 = dx * dx + dy * dy

    idx = int(np.argmin(dist2))
    dist_m = int(round(math.sqrt(float(dist2[idx]))))

    return (
        str(codes[idx]),
        str(names[idx]),
        str(sigungus[idx]),
        dist_m,
    )


# =========================
# Upsert
# =========================
def upsert_store_trade_area(engine, rows):
    """
    rows: list of dict
    - store_id (PK)
    - trdar_cd (nullable)
    - trdar_cd_nm (nullable)
    - sigungu_cd_nm (nullable)
    - match_method
    - distance_m (nullable)
    - matched_at, updated_at
    """
    sql = text("""
        INSERT INTO store_trade_area
            (store_id, trdar_cd, trdar_cd_nm, sigungu_cd_nm, match_method, distance_m, matched_at, updated_at)
        VALUES
            (:store_id, :trdar_cd, :trdar_cd_nm, :sigungu_cd_nm, :match_method, :distance_m, :matched_at, :updated_at)
        ON DUPLICATE KEY UPDATE
            trdar_cd      = VALUES(trdar_cd),
            trdar_cd_nm   = VALUES(trdar_cd_nm),
            sigungu_cd_nm = VALUES(sigungu_cd_nm),
            match_method  = VALUES(match_method),
            distance_m    = VALUES(distance_m),
            matched_at    = VALUES(matched_at),
            updated_at    = VALUES(updated_at)
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)


# =========================
# Main
# =========================
def main():
    print("[1/4] Loading trade areas from Mongo...")
    codes, names, sigungus, xs, ys = load_trade_areas()
    print(f"  - trade areas: {len(codes)}")

    print("[2/4] Loading stores from MySQL...")
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)
    stores = load_stores(engine)
    print(f"  - stores: {len(stores)}")

    print("[3/4] Matching nearest trade area...")
    now = datetime.now(timezone.utc).astimezone()

    payload = []
    mapped = 0
    out_of_range = 0

    for store_id, lat, lng in stores:
        trdar_cd, trdar_cd_nm, sigungu_cd_nm, dist_m = nearest_trade_area(
            lat, lng, codes, names, sigungus, xs, ys
        )

        # ✅ 컷오프: 너무 멀면 “미매핑”으로 기록
        if dist_m > MAX_DISTANCE_M:
            out_of_range += 1
            payload.append({
                "store_id": store_id,
                "trdar_cd": None,
                "trdar_cd_nm": None,
                "sigungu_cd_nm": None,
                "match_method": "OUT_OF_RANGE",
                "distance_m": dist_m,
                "matched_at": now,
                "updated_at": now,
            })
            continue

        mapped += 1
        payload.append({
            "store_id": store_id,
            "trdar_cd": trdar_cd,
            "trdar_cd_nm": trdar_cd_nm,
            "sigungu_cd_nm": sigungu_cd_nm,
            "match_method": "NEAREST",
            "distance_m": dist_m,
            "matched_at": now,
            "updated_at": now,
        })

    print(f"  - mapped: {mapped}, out_of_range: {out_of_range} (MAX_DISTANCE_M={MAX_DISTANCE_M})")

    print("[4/4] Upserting to MySQL store_trade_area...")
    upsert_store_trade_area(engine, payload)
    print("✅ Done. upsert rows:", len(payload))


if __name__ == "__main__":
    main()
