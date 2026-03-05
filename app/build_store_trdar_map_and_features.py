import os
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from pymongo import MongoClient, UpdateOne, ASCENDING
from pyproj import Transformer
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# =========================
# Config
# =========================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017").strip()
MONGO_DB = os.getenv("MONGO_DB", "erp_ai").strip()
MYSQL_URL = os.getenv("MYSQL_URL", "").strip()

if not MYSQL_URL:
    raise RuntimeError("MYSQL_URL이 비어있습니다. .env에 MYSQL_URL을 설정하세요.")

mongo = MongoClient(MONGO_URI)
db = mongo[MONGO_DB]

COL_AREA = db["seoul_trdar_area_raw"]          # 상권 영역(중심점)
COL_SALES = db["seoul_trdar_sales_raw"]        # 추정매출(분기, 상권x업종)
COL_CHANGE = db["seoul_trdar_change_raw"]      # 상권변화지표(분기, 상권)
COL_MAP = db["store_trdar_map"]                # ✅ store_id -> TRDAR_CD 매핑 결과
COL_FEAT = db["store_public_features"]         # ✅ store_id 단위 Feature View

# WGS84(lat/lng) -> EPSG:5181 (서울 투영좌표로 변환)
WGS84_TO_5181 = Transformer.from_crs("EPSG:4326", "EPSG:5181", always_xy=True)

# =========================
# Mongo indexes
# =========================
def ensure_indexes():
    COL_MAP.create_index([("store_id", ASCENDING)], unique=True)
    COL_MAP.create_index([("trdar_cd", ASCENDING)])
    COL_FEAT.create_index([("store_id", ASCENDING), ("quarter", ASCENDING)], unique=True)

# =========================
# Load stores from MySQL
# =========================
def load_store_points_from_mysql() -> List[Dict[str, Any]]:
    """
    너 ERP의 store_gps 테이블에서 store_id, latitude, longitude 가져오는 버전.
    컬럼명은 프로젝트마다 다를 수 있어서 필요하면 아래 SQL만 너 테이블명/컬럼명에 맞게 수정해줘.
    """
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)

    # ✅ 너 프로젝트에서 흔한 형태: store_gps(store_id, latitude, longitude)
    sql = text("""
        SELECT store_id, latitude AS lat, longitude AS lng
        FROM store_gps
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
    """)

    rows = []
    with engine.connect() as conn:
        for r in conn.execute(sql).mappings():
            rows.append({"store_id": int(r["store_id"]), "lat": float(r["lat"]), "lng": float(r["lng"])})
    return rows

# =========================
# Load TRDAR center points from Mongo
# =========================
def load_trdar_centers() -> List[Dict[str, Any]]:
    """
    seoul_trdar_area_raw 에서 상권 중심점 로딩
    """
    trdars = []
    for d in COL_AREA.find({}, {"TRDAR_CD": 1, "TRDAR_CD_NM": 1, "XCNTS_VALUE": 1, "YDNTS_VALUE": 1, "SIGNGU_CD_NM": 1, "TRDAR_SE_CD_NM": 1}):
        try:
            x = float(d.get("XCNTS_VALUE"))
            y = float(d.get("YDNTS_VALUE"))
        except:
            continue
        trdars.append({
            "trdar_cd": d.get("TRDAR_CD"),
            "trdar_nm": d.get("TRDAR_CD_NM"),
            "x": x,
            "y": y,
            "sigungu": d.get("SIGNGU_CD_NM"),
            "trdar_type": d.get("TRDAR_SE_CD_NM"),
        })
    return trdars

# =========================
# Nearest mapping
# =========================
def nearest_trdar(lat: float, lng: float, trdars: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], float]:
    """
    매장 1개에 대해 가장 가까운 상권(중심점) 찾기
    반환: (trdar_doc, 거리m)
    """
    sx, sy = WGS84_TO_5181.transform(lng, lat)

    best = None
    best_dist = 10**18
    for t in trdars:
        dx = t["x"] - sx
        dy = t["y"] - sy
        dist = (dx*dx + dy*dy) ** 0.5
        if dist < best_dist:
            best_dist = dist
            best = t
    return best, float(best_dist)

def build_store_trdar_map(radius_m_for_flag: float = 2000.0) -> int:
    """
    store_id -> nearest TRDAR_CD 매핑을 Mongo에 upsert 저장.
    radius_m_for_flag는 '2km 이내면 근처로 간주' 같은 플래그용(매핑은 nearest로 항상 됨)
    """
    stores = load_store_points_from_mysql()
    trdars = load_trdar_centers()

    now = datetime.utcnow().isoformat()
    ops = []
    for s in stores:
        t, dist = nearest_trdar(s["lat"], s["lng"], trdars)
        doc = {
            "store_id": s["store_id"],
            "store_lat": s["lat"],
            "store_lng": s["lng"],
            "trdar_cd": t["trdar_cd"],
            "trdar_nm": t["trdar_nm"],
            "sigungu": t["sigungu"],
            "trdar_type": t["trdar_type"],
            "distance_m": round(dist, 1),
            "is_within_2km": dist <= radius_m_for_flag,
            "method": "NEAREST_CENTER",
            "updated_at": now,
        }
        ops.append(UpdateOne({"store_id": s["store_id"]}, {"$set": doc}, upsert=True))

    if ops:
        COL_MAP.bulk_write(ops, ordered=False)
    return len(ops)

# =========================
# Build store-level public features
# =========================
def latest_available_quarter() -> Optional[str]:
    """
    change_raw 기준으로 최신 분기 하나 가져오기 (데이터 있는 분기만)
    """
    doc = COL_CHANGE.find_one(sort=[("STDR_YYQU_CD", -1)])
    return doc.get("STDR_YYQU_CD") if doc else None

def build_store_public_features(
    quarter: Optional[str] = None,
    svc_induty_cd: Optional[str] = None
) -> int:
    """
    store_id 기준 Feature View 생성:
      - change(상권변화지표): 상권 단위 1행
      - sales(추정매출): 상권x업종 단위 -> 업종코드로 1행(또는 여러개면 집계)
    svc_induty_cd가 없으면 sales feature는 비워둠(일단 change만 붙여도 됨)
    """
    q = quarter or latest_available_quarter()
    if not q:
        print("[FEATURE] No quarter data found.")
        return 0

    # 매장->상권 매핑 전부 읽기
    maps = list(COL_MAP.find({}, {"store_id": 1, "trdar_cd": 1, "trdar_nm": 1, "distance_m": 1, "is_within_2km": 1}))
    if not maps:
        print("[FEATURE] store_trdar_map is empty. Run mapping first.")
        return 0

    now = datetime.utcnow().isoformat()

    # change를 상권별로 미리 딕셔너리화(빠르게)
    change_docs = COL_CHANGE.find({"STDR_YYQU_CD": q}, {"TRDAR_CD": 1, "TRDAR_CHNGE_IX": 1, "TRDAR_CHNGE_IX_NM": 1,
                                                 "CLS_SALE_MT_AVRG": 1, "OPR_SALE_MT_AVRG": 1,
                                                 "SU_CLS_SALE_MT_AVRG": 1, "SU_OPR_SALE_MT_AVRG": 1})
    change_by_trdar = {d["TRDAR_CD"]: d for d in change_docs}

    sales_by_trdar = {}
    if svc_induty_cd:
        # sales는 상권x업종이라 (TRDAR_CD, STDR_YYQU_CD, SVC_INDUTY_CD)로 하나 고르는 구조
        sales_docs = COL_SALES.find({"STDR_YYQU_CD": q, "SVC_INDUTY_CD": svc_induty_cd},
                                    {"TRDAR_CD": 1, "THSMON_SELNG_AMT": 1, "THSMON_SELNG_CO": 1,
                                     "WKEND_SELNG_AMT": 1, "MDWK_SELNG_AMT": 1,
                                     "TMZON_11_14_SELNG_AMT": 1, "TMZON_17_21_SELNG_AMT": 1,
                                     "TMZON_21_24_SELNG_AMT": 1, "SVC_INDUTY_CD_NM": 1})
        sales_by_trdar = {d["TRDAR_CD"]: d for d in sales_docs}

    ops = []
    for m in maps:
        store_id = m["store_id"]
        trdar_cd = m["trdar_cd"]

        feat = {
            "store_id": store_id,
            "quarter": q,
            "trdar_cd": trdar_cd,
            "trdar_nm": m.get("trdar_nm"),
            "distance_m": m.get("distance_m"),
            "is_within_2km": m.get("is_within_2km"),
            "updated_at": now,
        }

        # ✅ change feature
        ch = change_by_trdar.get(trdar_cd)
        if ch:
            feat["trdar_change_ix"] = ch.get("TRDAR_CHNGE_IX")
            feat["trdar_change_ix_nm"] = ch.get("TRDAR_CHNGE_IX_NM")
            feat["cls_sale_mt_avrg"] = ch.get("CLS_SALE_MT_AVRG")
            feat["opr_sale_mt_avrg"] = ch.get("OPR_SALE_MT_AVRG")
            feat["su_cls_sale_mt_avrg"] = ch.get("SU_CLS_SALE_MT_AVRG")
            feat["su_opr_sale_mt_avrg"] = ch.get("SU_OPR_SALE_MT_AVRG")

        # ✅ sales feature (업종코드 지정했을 때만)
        sd = sales_by_trdar.get(trdar_cd) if svc_induty_cd else None
        if sd:
            feat["svc_induty_cd"] = svc_induty_cd
            feat["svc_induty_nm"] = sd.get("SVC_INDUTY_CD_NM")
            feat["thsm_selng_amt_q"] = sd.get("THSMON_SELNG_AMT")  # 분기 총액 성격(원 데이터 기준)
            feat["thsm_selng_cnt_q"] = sd.get("THSMON_SELNG_CO")
            feat["wkend_selng_amt"] = sd.get("WKEND_SELNG_AMT")
            feat["mdwk_selng_amt"] = sd.get("MDWK_SELNG_AMT")
            feat["tm_11_14_amt"] = sd.get("TMZON_11_14_SELNG_AMT")
            feat["tm_17_21_amt"] = sd.get("TMZON_17_21_SELNG_AMT")
            feat["tm_21_24_amt"] = sd.get("TMZON_21_24_SELNG_AMT")

        ops.append(UpdateOne({"store_id": store_id, "quarter": q}, {"$set": feat}, upsert=True))

    if ops:
        COL_FEAT.bulk_write(ops, ordered=False)

    return len(ops)

# =========================
# Main
# =========================
if __name__ == "__main__":
    print(f"[INIT] Mongo={MONGO_URI} DB={MONGO_DB}")
    ensure_indexes()

    n = build_store_trdar_map(radius_m_for_flag=2000.0)
    print(f"[MAP] upserted stores: {n}")

    # ✅ 업종 매핑이 아직 없으면 일단 None으로 두고 change feature만 붙여도 됨
    # 업종코드 테스트를 하고 싶으면 예: svc_induty_cd="CS100003"
    m = build_store_public_features(quarter=None, svc_induty_cd=None)
    print(f"[FEATURE] upserted features: {m}")

    print("[DONE] mapping + feature build finished.")
