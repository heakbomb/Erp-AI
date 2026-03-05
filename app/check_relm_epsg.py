import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pyproj import Transformer

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "erp_public_data")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
client.admin.command("ping")
db = client[MONGO_DB]
col = db["public_data_rows"]

# TbgisTrdarRelm 샘플 1개
doc = col.find_one(
    {"source": "SEOUL", "dataset": "TbgisTrdarRelm", "XCNTS_VALUE": {"$exists": True}, "YDNTS_VALUE": {"$exists": True}},
    {"_id": 0, "TRDAR_CD": 1, "TRDAR_CD_NM": 1, "XCNTS_VALUE": 1, "YDNTS_VALUE": 1}
)
if not doc:
    raise RuntimeError("TbgisTrdarRelm 샘플을 못 찾았습니다.")

x = float(doc["XCNTS_VALUE"])
y = float(doc["YDNTS_VALUE"])
print("SAMPLE:", doc["TRDAR_CD"], doc.get("TRDAR_CD_NM"), "x,y=", x, y)

# 후보 EPSG들(한국 TM에서 자주 쓰는 것들)
candidates = ["EPSG:5179", "EPSG:5181", "EPSG:5186", "EPSG:5174", "EPSG:5187"]

for epsg in candidates:
    try:
        tr = Transformer.from_crs(epsg, "EPSG:4326", always_xy=True)  # (x,y)->(lon,lat)
        lon, lat = tr.transform(x, y)
        # 대충 서울/수도권 범위로 필터링해서 보기 좋게 출력
        ok = (36.5 <= lat <= 38.5) and (125.5 <= lon <= 128.5)
        print(f"{epsg}: lat={lat:.6f}, lon={lon:.6f}  {'<-- plausible' if ok else ''}")
    except Exception as e:
        print(f"{epsg}: FAIL {e}")
