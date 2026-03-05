import random
import math
import csv
import json
from datetime import datetime, timedelta, date
from collections import defaultdict

# =========================================================
# 1) CONFIG
# =========================================================
SEED = 42
random.seed(SEED)

NUM_DAYS = 60
START_DATE = datetime.now().date() - timedelta(days=NUM_DAYS)

# ✅ 위치 제한: 종로구/강남구만
GU_LIST = [
    ("강남구", 37.4979, 127.0276),
    ("종로구", 37.5720, 126.9794),
]

# ✅ 구당 15개, 한식 10 / 치킨 5
STORES_PER_GU = 15
KOREAN_PER_GU = 10
CHICKEN_PER_GU = 5

IND_KOREAN = "KOREAN"
IND_CHICKEN = "CHICKEN"

# ✅ 마진 음수 방지 + 최소 마진(원가 기준)
MIN_MARGIN_RATE = 0.10
PRICE_ROUND_UNIT = 100

# ✅ 가격 높은 매장 30~50 / 낮은 매장 50~80
HIGH_PRICE_THRESHOLD = 15000

# ✅ 직원 생성/배정 설정
EMPLOYEES_PER_STORE_MIN = 2
EMPLOYEES_PER_STORE_MAX = 6
EMP_PROVIDERS = ["KAKAO", "NAVER", "GOOGLE"]
EMP_ROLES = ["STAFF", "MANAGER"]  # employee_assignment.role (varchar)

# ✅ SHIFT/급여 설정(단순화)
SHIFT_STARTS = [10, 11, 12, 13, 14, 15, 16]
SHIFT_HOURS = [4, 5, 6, 7, 8]
BREAK_MINUTES_CHOICES = [0, 15, 30, 45]

# payroll_history.status / payroll_run.status 예시 문자열(너 enum/상수에 맞게 바꿔도 됨)
PAYROLL_RUN_STATUS = "CALCULATED"
PAYROLL_HISTORY_STATUS = "CALCULATED"
PAYROLL_RUN_SOURCE = "DUMMY"

# ✅ store_neighbor 반경
NEIGHBOR_RADII = [500]  # 필요하면 [300, 500, 1000] 처럼 확장

# ✅ IDs (기존 데이터 충돌 방지)
ids = {
    "owner": 101,
    "biz": 101,
    "store": 101,
    "store_gps": 101,
    "employee": 101,
    "assignment": 1001,
    "shift": 10001,
    "payroll_setting": 10001,
    "payroll_run": 10001,
    "payroll_history": 10001,
    "item": 1001,
    "menu": 1001,
    "recipe": 1001,
    "purchase": 10001,
    "trans": 10001,
    "line": 10001,
    "snap": 10001,
    "summary": 10001,
    "menu_summary": 10001,
    "ml_feat": 10001,
    "ml_forecast": 10001,
}

CHUNK_SIZE = 5000

# =========================================================
# 2) UNIT NORMALIZATION
# =========================================================
WEIGHT_UNITS = {"g": 1.0, "kg": 1000.0}
VOLUME_UNITS = {"ml": 1.0, "l": 1000.0}


def normalize_qty_to_base(qty: float, unit: str):
    u = unit.strip().lower()
    if u in WEIGHT_UNITS:
        return qty * WEIGHT_UNITS[u], "G"
    if u in VOLUME_UNITS:
        return qty * VOLUME_UNITS[u], "ML"
    if u in {"ea", "개", "pcs"}:
        return float(qty), "EA"
    if u in {"bottle", "can"}:
        return float(qty), u.upper()  # BOTTLE / CAN
    return float(qty), "OTHER"


def stock_type_from_base(base_unit: str):
    if base_unit in {"G", "ML", "EA", "BOTTLE", "CAN"}:
        return base_unit
    return "EA"


# =========================================================
# 3) RECIPE "말도 안되는 수치" 방지
# =========================================================
MAX_PER_SERVING = {
    "G": 1000.0,
    "ML": 1000.0,
    "EA": 10.0,
    "BOTTLE": 5.0,
    "CAN": 5.0,
    "OTHER": 1000.0,
}


def clamp_serving_qty(qty_base: float, base_unit: str):
    cap = MAX_PER_SERVING.get(base_unit, 1000.0)
    qty_base = max(0.0, min(qty_base, cap))
    if base_unit in {"G", "ML"}:
        return round(qty_base, 3)
    q = int(round(qty_base))
    return q if q > 0 else 1


# =========================================================
# 4) MASTER DATA
# =========================================================
INGREDIENTS = {
    "돼지고기_뒷다리": {"pack_qty": (10000, "g"), "pack_cost": 60000, "cat": "MEAT"},
    "고등어_자반": {"pack_qty": (20, "ea"), "pack_cost": 40000, "cat": "SEAFOOD"},
    "소불고기": {"pack_qty": (5000, "g"), "pack_cost": 80000, "cat": "MEAT"},
    "토종순대": {"pack_qty": (2000, "g"), "pack_cost": 15000, "cat": "PROCESSED_FOOD"},
    "돼지목살": {"pack_qty": (10000, "g"), "pack_cost": 140000, "cat": "MEAT"},
    "김치": {"pack_qty": (10000, "g"), "pack_cost": 25000, "cat": "VEGETABLE"},
    "두부": {"pack_qty": (3000, "g"), "pack_cost": 6000, "cat": "PROCESSED_FOOD"},
    "염지닭": {"pack_qty": (10, "ea"), "pack_cost": 50000, "cat": "MEAT"},
    "냉동치즈볼": {"pack_qty": (30, "ea"), "pack_cost": 12000, "cat": "FROZEN"},
    "양파": {"pack_qty": (15000, "g"), "pack_cost": 20000, "cat": "VEGETABLE"},
    "대파": {"pack_qty": (1000, "g"), "pack_cost": 3000, "cat": "VEGETABLE"},
    "당근": {"pack_qty": (10000, "g"), "pack_cost": 25000, "cat": "VEGETABLE"},
    "팽이버섯": {"pack_qty": (5000, "g"), "pack_cost": 20000, "cat": "VEGETABLE"},
    "마늘": {"pack_qty": (1000, "g"), "pack_cost": 8000, "cat": "VEGETABLE"},
    "제육양념": {"pack_qty": (2000, "g"), "pack_cost": 10000, "cat": "SEASONING"},
    "불고기양념": {"pack_qty": (2000, "g"), "pack_cost": 10000, "cat": "SEASONING"},
    "식용유": {"pack_qty": (18, "l"), "pack_cost": 50000, "cat": "SEASONING"},
    "사골육수": {"pack_qty": (10, "l"), "pack_cost": 20000, "cat": "SEASONING"},
    "들깨가루": {"pack_qty": (1000, "g"), "pack_cost": 12000, "cat": "SEASONING"},
    "다데기": {"pack_qty": (2000, "g"), "pack_cost": 15000, "cat": "SEASONING"},
    "크리스피파우더": {"pack_qty": (5000, "g"), "pack_cost": 15000, "cat": "SEASONING"},
    "양념소스": {"pack_qty": (10000, "g"), "pack_cost": 25000, "cat": "SEASONING"},
    "마늘간장소스": {"pack_qty": (2000, "g"), "pack_cost": 15000, "cat": "SEASONING"},
    "공기밥": {"pack_qty": (200, "ea"), "pack_cost": 50000, "cat": "GRAIN"},
    "계란": {"pack_qty": (30, "ea"), "pack_cost": 7000, "cat": "ETC"},
    "소주": {"pack_qty": (20, "bottle"), "pack_cost": 25000, "cat": "ETC"},
    "맥주": {"pack_qty": (20, "bottle"), "pack_cost": 30000, "cat": "ETC"},
    "생맥주케그": {"pack_qty": (20, "l"), "pack_cost": 60000, "cat": "ETC"},
    "콜라": {"pack_qty": (24, "can"), "pack_cost": 15000, "cat": "ETC"},
    "사이다": {"pack_qty": (24, "can"), "pack_cost": 15000, "cat": "ETC"},
    "튀김가루": {"pack_qty": (1000, "g"), "pack_cost": 3000, "cat": "SEASONING"},
    "당면": {"pack_qty": (1000, "g"), "pack_cost": 4000, "cat": "GRAIN"},
}

MENUS = {
    IND_KOREAN: [
        {"name": "제육 정식", "price": 10000, "cat": "밥/정식", "sub": "제육정식",
         "recipe": [("돼지고기_뒷다리", 200, "g"), ("양파", 50, "g"), ("제육양념", 30, "g"), ("공기밥", 1, "ea")]},
        {"name": "생선구이 정식", "price": 11000, "cat": "밥/정식", "sub": "생선구이정식",
         "recipe": [("고등어_자반", 1, "ea"), ("식용유", 30, "ml"), ("공기밥", 1, "ea")]},
        {"name": "불고기 뚝배기", "price": 11000, "cat": "밥/정식", "sub": "불고기정식",
         "recipe": [("소불고기", 150, "g"), ("불고기양념", 40, "g"), ("팽이버섯", 20, "g"), ("당면", 20, "g")]},
        {"name": "순대국밥", "price": 9000, "cat": "국/탕", "sub": "순대국",
         "recipe": [("토종순대", 100, "g"), ("사골육수", 400, "ml"), ("들깨가루", 10, "g"), ("다데기", 15, "g")]},
        {"name": "김치찌개", "price": 9000, "cat": "찌개/전골", "sub": "김치찌개",
         "recipe": [("김치", 150, "g"), ("돼지목살", 50, "g"), ("두부", 50, "g")]},
        {"name": "계란말이", "price": 8000, "cat": "사이드", "sub": "계란말이",
         "recipe": [("계란", 5, "ea"), ("당근", 20, "g"), ("대파", 20, "g")]},
        {"name": "두부김치", "price": 12000, "cat": "사이드", "sub": "두부김치", "recipe": [("두부", 200, "g"), ("김치", 150, "g")]},
        {"name": "소주", "price": 5000, "cat": "주류/음료", "sub": "소주", "recipe": [("소주", 1, "bottle")]},
        {"name": "맥주", "price": 5000, "cat": "주류/음료", "sub": "맥주", "recipe": [("맥주", 1, "bottle")]},
        {"name": "콜라", "price": 2000, "cat": "주류/음료", "sub": "음료", "recipe": [("콜라", 1, "can")]},
    ],
    IND_CHICKEN: [
        {"name": "크리스피 치킨", "price": 18000, "cat": "후라이드", "sub": "크리스피",
         "recipe": [("염지닭", 1, "ea"), ("크리스피파우더", 100, "g"), ("식용유", 500, "ml")]},
        {"name": "반반 치킨", "price": 19000, "cat": "후라이드", "sub": "반반치킨",
         "recipe": [("염지닭", 1, "ea"), ("크리스피파우더", 100, "g"), ("양념소스", 50, "g")]},
        {"name": "양념 치킨", "price": 19000, "cat": "양념/간장", "sub": "양념치킨",
         "recipe": [("염지닭", 1, "ea"), ("크리스피파우더", 100, "g"), ("양념소스", 100, "g")]},
        {"name": "마늘간장 치킨", "price": 19000, "cat": "양념/간장", "sub": "마늘간장",
         "recipe": [("염지닭", 1, "ea"), ("튀김가루", 100, "g"), ("마늘간장소스", 80, "g")]},
        {"name": "치즈볼", "price": 5000, "cat": "사이드", "sub": "치즈볼", "recipe": [("냉동치즈볼", 5, "ea")]},
        {"name": "생맥주 500cc", "price": 4500, "cat": "주류/음료", "sub": "맥주", "recipe": [("생맥주케그", 500, "ml")]},
        {"name": "소주", "price": 5000, "cat": "주류/음료", "sub": "소주", "recipe": [("소주", 1, "bottle")]},
        {"name": "사이다", "price": 2000, "cat": "주류/음료", "sub": "음료", "recipe": [("사이다", 1, "can")]},
    ],
}

# =========================================================
# 5) SQL HELPERS
# =========================================================
sql_statements = []


def add_sql(stmt: str):
    sql_statements.append(stmt)


def sql_escape(s: str) -> str:
    return s.replace("\\", "\\\\").replace("'", "''")


def round_price(v: float) -> int:
    return int(round(v / PRICE_ROUND_UNIT) * PRICE_ROUND_UNIT)


def menu_price_guard(price: float, cost: float) -> float:
    # 최소 마진 보장
    min_price = cost * (1.0 + MIN_MARGIN_RATE)
    guarded = max(price, math.ceil(min_price))
    return float(round_price(guarded))


def write_chunks(table, cols, rows):
    if not rows:
        return
    for i in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[i:i + CHUNK_SIZE]
        add_sql(f"INSERT INTO {table} {cols} VALUES\n" + ",\n".join(chunk) + ";")


def jitter_latlon(lat, lon, meters=700):
    dlat = (random.uniform(-meters, meters) / 111000.0)
    dlon = (random.uniform(-meters, meters) / (111000.0 * math.cos(math.radians(lat))))
    return lat + dlat, lon + dlon


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return int(round(2 * R * math.asin(math.sqrt(a))))


def ym_of(d: date) -> str:
    return f"{d.year}-{str(d.month).zfill(2)}"


def next_ym(ym: str) -> str:
    y, m = ym.split("-")
    y = int(y); m = int(m)
    if m == 12:
        return f"{y+1}-01"
    return f"{y}-{str(m+1).zfill(2)}"


# =========================================================
# 6) OWNERS (사장당 1~3개 사업장 랜덤, 총 30개 맞추기)
# =========================================================
TOTAL_STORES = len(GU_LIST) * STORES_PER_GU  # 30

owner_store_counts = []
remain = TOTAL_STORES
while remain > 0:
    c = random.randint(1, 3)
    if c > remain:
        c = remain
    owner_store_counts.append(c)
    remain -= c

owners = []
for _ in range(len(owner_store_counts)):
    oid = ids["owner"]; ids["owner"] += 1
    username = f"owner_{oid}_new"
    email_prefix = f"owner_{oid}"
    owners.append((oid, username, email_prefix))

add_sql("INSERT INTO owner (owner_id, username, password, salt, email, created_at) VALUES")
add_sql(",\n".join([
    f"({o[0]}, '{sql_escape(o[1])}', 'password', 'dummy_salt', '{sql_escape(o[2])}@example.com', NOW(6))"
    for o in owners
]) + ";")

owner_slots = []
for idx, cnt in enumerate(owner_store_counts):
    owner_slots.extend([owners[idx][0]] * cnt)
random.shuffle(owner_slots)

# =========================================================
# 7) STORE BLUEPRINT (강남/종로, 각 15개(한식10/치킨5))
# =========================================================
store_blueprints = []
for gu_name, c_lat, c_lon in GU_LIST:
    for i in range(KOREAN_PER_GU):
        store_blueprints.append({"gu": gu_name, "name": f"{gu_name} 한식 {i+1}호점(New)", "industry": IND_KOREAN, "center_lat": c_lat, "center_lon": c_lon})
    for i in range(CHICKEN_PER_GU):
        store_blueprints.append({"gu": gu_name, "name": f"{gu_name} 치킨 {i+1}호점(New)", "industry": IND_CHICKEN, "center_lat": c_lat, "center_lon": c_lon})

# =========================================================
# 8) BUSINESS_NUMBER / STORE / STORE_GPS
#    ✅ store.status = APPROVED (요청)
# =========================================================
biz_rows, store_rows, gps_rows = [], [], []
stores_meta = []
store_latlon = {}

for bp, owner_id in zip(store_blueprints, owner_slots):
    biz_id = ids["biz"]; ids["biz"] += 1
    biz_num_str = f"10000{biz_id}"
    biz_rows.append(f"({biz_id}, {owner_id}, '010-0000-0000', '{biz_num_str}', '영업중', '일반과세자', NULL, NOW(6))")

    s_id = ids["store"]; ids["store"] += 1
    store_rows.append(
        f"({s_id}, b'1', NOW(6), '{bp['industry']}', 'POS_V1', 'APPROVED', '{sql_escape(bp['name'])}', {biz_id})"
    )

    lat, lon = jitter_latlon(bp["center_lat"], bp["center_lon"], meters=700)
    store_latlon[s_id] = (lat, lon)

    gps_id = ids["store_gps"]; ids["store_gps"] += 1
    # nx/ny는 임시(원하면 위경도→격자 변환 로직 넣어도 됨)
    gps_rows.append(f"({gps_id}, {s_id}, {lat:.8f}, {lon:.8f}, 60, 127, 500)")

    stores_meta.append({"store_id": s_id, "gu": bp["gu"], "industry": bp["industry"]})

add_sql("INSERT INTO business_number (biz_id, owner_id, phone, biz_num, open_status, tax_type, end_dt, certified_at) VALUES")
add_sql(",\n".join(biz_rows) + ";")

add_sql("INSERT INTO store (store_id, active, approved_at, industry, pos_vendor, status, store_name, biz_id) VALUES")
add_sql(",\n".join(store_rows) + ";")

add_sql("INSERT INTO store_gps (store_gps_id, store_id, latitude, longitude, nx, ny, gps_radius_m) VALUES")
add_sql(",\n".join(gps_rows) + ";")

# =========================================================
# 8.1) store_trade_area (간단 매핑 생성)
# =========================================================
# 실제 상권코드/명은 네 Mongo/공공데이터에서 가져오겠지만, 더미로는 아래처럼
TRADE_AREAS_BY_GU = {
    "강남구": [("TA-GN-001", "강남역"), ("TA-GN-002", "선릉역"), ("TA-GN-003", "논현역")],
    "종로구": [("TA-JR-001", "광화문"), ("TA-JR-002", "종각"), ("TA-JR-003", "혜화")],
}

sta_rows = []
for sm in stores_meta:
    s_id = sm["store_id"]
    gu = sm["gu"]
    trdar_cd, trdar_nm = random.choice(TRADE_AREAS_BY_GU[gu])
    # DDL: (store_id, trdar_cd, trdar_cd_nm, sigungu_cd_nm, match_method, distance_m, matched_at, updated_at)
    sta_rows.append(
        f"({s_id}, '{trdar_cd}', '{sql_escape(trdar_nm)}', '{sql_escape(gu)}', 'NEAREST', {random.randint(50, 800)}, NOW(6), NOW(6))"
    )

add_sql("INSERT INTO store_trade_area (store_id, trdar_cd, trdar_cd_nm, sigungu_cd_nm, match_method, distance_m, matched_at, updated_at) VALUES")
add_sql(",\n".join(sta_rows) + ";")

# =========================================================
# 8.2) store_neighbor (GPS 기반 거리 계산)
# =========================================================
sn_rows = []
for radius in NEIGHBOR_RADII:
    for a in stores_meta:
        a_id = a["store_id"]
        lat1, lon1 = store_latlon[a_id]
        for b in stores_meta:
            b_id = b["store_id"]
            if a_id == b_id:
                continue
            lat2, lon2 = store_latlon[b_id]
            dist = haversine_m(lat1, lon1, lat2, lon2)
            if dist <= radius:
                # DDL: (neighbor_store_id, radius_m, store_id, created_at, distance_m, updated_at)
                sn_rows.append(f"({b_id}, {radius}, {a_id}, NOW(6), {dist}, NOW(6))")

# 중복이 있을 수 있으니(set) 정리
sn_rows = list(dict.fromkeys(sn_rows))

if sn_rows:
    add_sql("INSERT INTO store_neighbor (neighbor_store_id, radius_m, store_id, created_at, distance_m, updated_at) VALUES")
    add_sql(",\n".join(sn_rows) + ";")

# =========================================================
# 9) EMPLOYEE / EMPLOYEE_ASSIGNMENT
# =========================================================
employee_rows, assignment_rows = [], []
store_employees = defaultdict(list)  # store_id -> [emp_id,...]
NAME_POOL = ["민수", "서연", "지훈", "지민", "현우", "수빈", "예진", "도윤", "하늘", "유진", "태형", "나연", "지수", "정우", "윤아"]
LASTNAME_POOL = ["김", "이", "박", "최", "정", "강", "조", "윤", "장", "임"]
provider_seq = 100000

for sm in stores_meta:
    s_id = sm["store_id"]
    n_emp = random.randint(EMPLOYEES_PER_STORE_MIN, EMPLOYEES_PER_STORE_MAX)

    for _ in range(n_emp):
        emp_id = ids["employee"]; ids["employee"] += 1
        name = random.choice(LASTNAME_POOL) + random.choice(NAME_POOL)
        provider = random.choice(EMP_PROVIDERS)
        provider_id = f"{provider.lower()}_{provider_seq}"
        provider_seq += 1
        email = f"{provider_id}@example.com"
        phone = f"010-{random.randint(1000,9999)}-{random.randint(1000,9999)}"

        employee_rows.append(f"({emp_id}, NOW(6), '{sql_escape(email)}', '{sql_escape(name)}', '{phone}', '{provider}', '{provider_id}')")
        store_employees[s_id].append(emp_id)

        assignment_id = ids["assignment"]; ids["assignment"] += 1
        role = random.choices(EMP_ROLES, weights=[0.8, 0.2])[0]
        assignment_rows.append(f"({assignment_id}, '{role}', 'APPROVED', {emp_id}, {s_id})")

add_sql("INSERT INTO employee (employee_id, created_at, email, name, phone, provider, provider_id) VALUES")
add_sql(",\n".join(employee_rows) + ";")

add_sql("INSERT INTO employee_assignment (assignment_id, role, status, employee_id, store_id) VALUES")
add_sql(",\n".join(assignment_rows) + ";")

# =========================================================
# 9.1) payroll_setting (직원별 급여 설정)
# =========================================================
ps_rows = []
emp_wage = {}  # emp_id -> base_wage
emp_wage_type = {}  # emp_id -> wage_type

for s_id, emp_ids in store_employees.items():
    for emp_id in emp_ids:
        setting_id = ids["payroll_setting"]; ids["payroll_setting"] += 1
        wage_type = "HOURLY"  # 단순화
        base_wage = random.choice([9860, 10000, 11000, 12000, 13000, 14000, 15000])
        emp_wage[emp_id] = base_wage
        emp_wage_type[emp_id] = wage_type

        # deduction_items는 null 또는 기본값(JSON). 여기서는 예시 기본 공제 2종.
        deduction_items = json.dumps([
            {"type": "FOUR_INSURANCE", "deductionType": "RATE", "rate": 0.09},
            {"type": "TAX_3_3", "deductionType": "RATE", "rate": 0.033}
        ], ensure_ascii=False)

        ps_rows.append(
            f"({setting_id}, {base_wage:.2f}, '{sql_escape(deduction_items)}', '{wage_type}', {emp_id}, {s_id})"
        )

add_sql("INSERT INTO payroll_setting (setting_id, base_wage, deduction_items, wage_type, employee_id, store_id) VALUES")
add_sql(",\n".join(ps_rows) + ";")

# =========================================================
# 10) INVENTORY / MENU / RECIPE
# =========================================================
inv_map = {}
menu_map = {}

for sm in stores_meta:
    s_id = sm["store_id"]
    industry = sm["industry"]
    inv_map[s_id] = {}

    needed = set()
    for m in MENUS[industry]:
        for iname, _, _ in m["recipe"]:
            needed.add(iname)

    for item_name in needed:
        info = INGREDIENTS[item_name]
        pack_qty_raw, pack_unit_raw = info["pack_qty"]
        pack_qty_base, base_unit = normalize_qty_to_base(pack_qty_raw, pack_unit_raw)
        unit_cost = info["pack_cost"] / pack_qty_base

        init_packs = random.randint(1, 5)
        init_qty = init_packs * pack_qty_base
        safety_qty = 0.5 * pack_qty_base

        item_id = ids["item"]; ids["item"] += 1
        inv_map[s_id][item_name] = {
            "id": item_id,
            "cat": info["cat"],
            "stock_type": stock_type_from_base(base_unit),
            "stock": float(init_qty),
            "safety": float(safety_qty),
            "unit_cost": float(unit_cost),
        }

    menu_map[s_id] = []
    for m in MENUS[industry]:
        m_id = ids["menu"]; ids["menu"] += 1
        norm_recipe = []
        cost = 0.0

        for iname, qty, unit in m["recipe"]:
            if iname not in inv_map[s_id]:
                continue
            qty_base, base_unit = normalize_qty_to_base(qty, unit)
            stype = stock_type_from_base(base_unit)
            qty_base = clamp_serving_qty(qty_base, stype)

            inv = inv_map[s_id][iname]
            cost += inv["unit_cost"] * qty_base
            norm_recipe.append((iname, qty_base))

        guarded_price = menu_price_guard(m["price"], cost)

        menu_map[s_id].append({
            "id": m_id,
            "name": m["name"],
            "price": guarded_price,
            "cost": round(cost, 4),
            "cat": m["cat"],
            "sub": m["sub"],
            "recipe": norm_recipe
        })

inv_rows = []
for s_id, items in inv_map.items():
    for name, data in items.items():
        inv_rows.append(
            f"({data['id']}, '{sql_escape(name)}', '{data['cat']}', {data['unit_cost']:.4f}, "
            f"{data['safety']:.3f}, 'ACTIVE', {data['stock']:.3f}, '{data['stock_type']}', {s_id})"
        )

add_sql("INSERT INTO inventory (item_id, item_name, item_type, last_unit_cost, safety_qty, status, stock_qty, stock_type, store_id) VALUES")
add_sql(",\n".join(inv_rows) + ";")

menu_rows, recipe_rows = [], []
for s_id, menus in menu_map.items():
    for m in menus:
        menu_rows.append(
            f"({m['id']}, {m['cost']:.4f}, '{sql_escape(m['cat'])}', '{sql_escape(m['name'])}', "
            f"{m['price']:.2f}, 'ACTIVE', '{sql_escape(m['sub'])}', {s_id})"
        )
        for iname, qty_base in m["recipe"]:
            item_id = inv_map[s_id][iname]["id"]
            recipe_rows.append(f"({ids['recipe']}, {m['id']}, {item_id}, {float(qty_base):.3f})")
            ids["recipe"] += 1

add_sql("INSERT INTO menu_item (menu_id, calculated_cost, category_name, menu_name, price, status, sub_category_name, store_id) VALUES")
add_sql(",\n".join(menu_rows) + ";")

add_sql("INSERT INTO recipe_ingredient (recipe_id, menu_id, item_id, consumption_qty) VALUES")
add_sql(",\n".join(recipe_rows) + ";")

# =========================================================
# 11) WEATHER FEATURES CSV (옵션)
# =========================================================
def seasonal_temp(d):
    doy = d.timetuple().tm_yday
    return 12.5 + 17.5 * math.sin(2 * math.pi * (doy - 200) / 365.0)


def gen_weather(gu_name, d):
    base_t = seasonal_temp(d)
    gu_offset = 0.3 if gu_name == "강남구" else -0.2
    temp = base_t + gu_offset + random.uniform(-2.0, 2.0)

    month = d.month
    if month in (6, 7, 8):
        rain_prob = 0.35
        rain_mm = random.uniform(0, 40) if random.random() < rain_prob else 0.0
        snow_cm = 0.0
    elif month in (12, 1, 2):
        snow_prob = 0.20
        snow_cm = random.uniform(0, 10) if random.random() < snow_prob else 0.0
        rain_mm = random.uniform(0, 10) if random.random() < 0.10 else 0.0
    else:
        rain_prob = 0.20
        rain_mm = random.uniform(0, 25) if random.random() < rain_prob else 0.0
        snow_cm = 0.0

    humidity = random.uniform(35, 85)
    is_event = 1 if random.random() < 0.30 else 0
    is_weekend = 1 if d.weekday() >= 5 else 0

    return {
        "gu": gu_name,
        "date": d.isoformat(),
        "temp_c": round(temp, 1),
        "rain_mm": round(rain_mm, 1),
        "snow_cm": round(snow_cm, 1),
        "humidity": round(humidity, 1),
        "is_weekend": is_weekend,
        "is_event": is_event,
    }


weather_rows = []
for day_offset in range(NUM_DAYS):
    d = START_DATE + timedelta(days=day_offset)
    for gu_name, _, _ in GU_LIST:
        weather_rows.append(gen_weather(gu_name, d))

with open("weather_daily_features.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=list(weather_rows[0].keys()))
    w.writeheader()
    w.writerows(weather_rows)

# =========================================================
# 12) SIMULATION (purchase / sales / summaries / snapshots)
#     + shifts/payroll 집계용 raw accumulator
# =========================================================
purchase_rows, trans_rows, line_rows, daily_rows, menu_daily_rows, snap_rows = [], [], [], [], [], []

# 월 집계용
month_sales = defaultdict(float)         # (store_id, ym) -> sales
month_tx_count = defaultdict(int)        # (store_id, ym) -> transactions
month_menu_count = defaultdict(int)      # (store_id, ym) -> menu_count (나중에 한번만 넣어도 됨)
month_avg_menu_price = {}                # (store_id, ym) -> avg menu price
month_cogs = defaultdict(float)          # (store_id, ym) -> cogs (레시피원가 기반 근사)
month_labor_gross = defaultdict(float)   # (store_id, ym) -> labor gross pay (shift로 계산)

store_price_bucket = {}
for sm in stores_meta:
    s_id = sm["store_id"]
    menus = menu_map[s_id]
    mains = [m for m in menus if m["cat"] not in ["사이드", "주류/음료"]] or menus
    avg_main_price = sum(float(m["price"]) for m in mains) / max(1, len(mains))
    store_price_bucket[s_id] = "HIGH" if avg_main_price >= HIGH_PRICE_THRESHOLD else "LOW"

print("Simulating 60 days...")

# ---- employee_shift 생성 (근무기록 -> payroll용) ----
shift_rows = []
# (emp_id, store_id, ym) -> work_minutes
work_minutes_by_emp_month = defaultdict(int)

for day_offset in range(NUM_DAYS):
    curr_date = START_DATE + timedelta(days=day_offset)
    date_str = curr_date.strftime("%Y-%m-%d")
    ym = ym_of(curr_date)

    weather_by_gu = {gu_name: gen_weather(gu_name, curr_date) for gu_name, _, _ in GU_LIST}

    # SHIFT: 매장별로 그날 1~3명 정도 근무하도록 단순 생성
    for sm in stores_meta:
        s_id = sm["store_id"]
        emp_ids = store_employees[s_id]
        if not emp_ids:
            continue

        # 이벤트/주말엔 근무자 조금 늘림
        wgu = weather_by_gu[sm["gu"]]
        n_workers = random.randint(1, min(3, len(emp_ids)))
        if wgu["is_weekend"] == 1 or wgu["is_event"] == 1:
            n_workers = min(len(emp_ids), n_workers + 1)

        workers = random.sample(emp_ids, k=n_workers)

        for emp_id in workers:
            sh_id = ids["shift"]; ids["shift"] += 1
            start_h = random.choice(SHIFT_STARTS)
            hours = random.choice(SHIFT_HOURS)
            end_h = min(23, start_h + hours)
            break_m = random.choice(BREAK_MINUTES_CHOICES)
            is_fixed = 1 if random.random() < 0.15 else 0

            # time(6) 포맷
            st = f"{start_h:02d}:00:00.000000"
            et = f"{end_h:02d}:00:00.000000"

            shift_rows.append(
                f"({sh_id}, {break_m}, NOW(6), '{et}', b'{is_fixed}', '{date_str}', NULL, '{st}', NOW(6), {emp_id}, {s_id})"
            )

            minutes = max(0, (end_h - start_h) * 60 - break_m)
            work_minutes_by_emp_month[(emp_id, s_id, ym)] += minutes

# employee_shift insert
add_sql("INSERT INTO employee_shift (shift_id, break_minutes, created_at, end_time, is_fixed, shift_date, shift_group_id, start_time, updated_at, employee_id, store_id) VALUES")
add_sql(",\n".join(shift_rows) + ";")

# ---- sales/purchase simulation ----
for day_offset in range(NUM_DAYS):
    curr_date = START_DATE + timedelta(days=day_offset)
    date_str = curr_date.strftime("%Y-%m-%d")
    ym = ym_of(curr_date)

    weather_by_gu = {gu_name: gen_weather(gu_name, curr_date) for gu_name, _, _ in GU_LIST}

    for sm in stores_meta:
        s_id = sm["store_id"]
        gu = sm["gu"]

        store_items = inv_map[s_id]
        store_menus = menu_map[s_id]
        daily_menu_stats = {}

        # ---- PURCHASE ----
        for name, data in store_items.items():
            should_buy = (data["stock"] < data["safety"]) or (day_offset % 3 == 0)
            if not should_buy:
                continue

            info = INGREDIENTS[name]
            pack_qty_raw, pack_unit_raw = info["pack_qty"]
            pack_qty_base, _ = normalize_qty_to_base(pack_qty_raw, pack_unit_raw)

            wgu = weather_by_gu[gu]
            bump = 1 if (wgu["is_event"] == 1 or wgu["rain_mm"] > 0) else 0

            qty_packs = random.randint(1, 3) + bump
            buy_qty = qty_packs * pack_qty_base

            unit_price = info["pack_cost"] / pack_qty_base

            # 재고 반영 + last_unit_cost 갱신(실제 서비스에선 평균원가 갱신 로직이겠지만 더미는 최신가로)
            data["stock"] += buy_qty
            data["unit_cost"] = unit_price

            # purchase_history: (purchase_id, purchase_date, purchase_qty, unit_price, item_id, store_id)
            purchase_rows.append(f"({ids['purchase']}, '{date_str}', {buy_qty:.3f}, {unit_price:.2f}, {data['id']}, {s_id})")
            ids["purchase"] += 1

        # ---- SALES ----
        bucket = store_price_bucket[s_id]
        base_cnt = random.randint(30, 50) if bucket == "HIGH" else random.randint(50, 80)

        wgu = weather_by_gu[gu]
        if wgu["is_event"] == 1:
            base_cnt = int(base_cnt * 1.15)
        if wgu["is_weekend"] == 1:
            base_cnt = int(base_cnt * 1.10)
        if wgu["rain_mm"] >= 20:
            base_cnt = int(base_cnt * 0.92)

        base_cnt = max(10, base_cnt)
        daily_total = 0.0
        daily_cogs = 0.0

        mains = [m for m in store_menus if m["cat"] not in ["사이드", "주류/음료"]] or store_menus
        drinks = [m for m in store_menus if m["cat"] == "주류/음료"]
        sides = [m for m in store_menus if m["cat"] == "사이드"]

        for _ in range(base_cnt):
            t_id = ids["trans"]; ids["trans"] += 1

            # 메인 종류 1~5
            main_types = random.randint(1, min(5, len(mains)))
            chosen_mains = random.sample(mains, k=main_types)

            selected_lines = []
            # 메인 판매 수량 1~5
            for mm in chosen_mains:
                selected_lines.append((mm, random.randint(1, 5)))

            # 음료 0~5
            if drinks:
                drink_types = random.randint(0, min(5, len(drinks)))
                if drink_types > 0:
                    for dm in random.sample(drinks, k=drink_types):
                        selected_lines.append((dm, random.randint(1, 3)))

            # 사이드 조금
            if sides and random.random() < 0.35:
                side_types = random.randint(1, min(2, len(sides)))
                for smenu in random.sample(sides, k=side_types):
                    selected_lines.append((smenu, random.randint(1, 2)))

            # 동일 메뉴 여러 라인 가능
            if random.random() < 0.20 and selected_lines:
                dup_menu, _ = random.choice(selected_lines)
                selected_lines.append((dup_menu, random.randint(1, 2)))

            t_amt = 0.0
            t_time = f"{date_str} {random.randint(11, 22):02d}:{random.randint(0, 59):02d}:00"

            for m, qty in selected_lines:
                line_total = float(m["price"]) * qty
                t_amt += line_total

                # sales_line_item: (line_id, line_amount, quantity, unit_price, menu_id, transaction_id)
                line_rows.append(f"({ids['line']}, {line_total:.2f}, {qty}, {float(m['price']):.2f}, {m['id']}, {t_id})")
                ids["line"] += 1

                # COGS 근사(레시피 cost * qty)
                daily_cogs += float(m["cost"]) * qty

                # 재고 차감
                for iname, cons_qty in m["recipe"]:
                    if iname in store_items:
                        store_items[iname]["stock"] -= (float(cons_qty) * qty)
                        if store_items[iname]["stock"] < 0:
                            store_items[iname]["stock"] = 0.0

                mid = m["id"]
                if mid not in daily_menu_stats:
                    daily_menu_stats[mid] = {"qty": 0, "amt": 0.0}
                daily_menu_stats[mid]["qty"] += qty
                daily_menu_stats[mid]["amt"] += line_total

            # sales_transaction:
            trans_rows.append(f"({t_id}, NULL, 'ORD-{t_id}', 'CARD', 'PAID', {t_amt:.2f}, 0.00, '{t_time}', {s_id})")
            daily_total += t_amt

        # 일 summary
        daily_rows.append(f"({ids['summary']}, {s_id}, '{date_str}', {daily_total:.2f}, {base_cnt})")
        ids["summary"] += 1

        # 메뉴 summary
        for mid, st in daily_menu_stats.items():
            menu_daily_rows.append(f"({ids['menu_summary']}, '{date_str}', {st['amt']:.2f}, {st['qty']}, {mid}, {s_id})")
            ids["menu_summary"] += 1

        # snapshot
        for _, data in store_items.items():
            snap_rows.append(f"({ids['snap']}, NOW(6), '{date_str}', {data['stock']:.3f}, {data['id']}, {s_id})")
            ids["snap"] += 1

        # 월 집계 누적
        month_sales[(s_id, ym)] += daily_total
        month_tx_count[(s_id, ym)] += base_cnt
        month_cogs[(s_id, ym)] += daily_cogs

# =========================================================
# 12.1) labor 월 집계 (shift 기반)
# =========================================================
for (emp_id, s_id, ym), minutes in work_minutes_by_emp_month.items():
    wage = emp_wage.get(emp_id, 10000)
    gross = (minutes / 60.0) * wage
    month_labor_gross[(s_id, ym)] += gross

# =========================================================
# 13) CHUNK INSERTS (✅ DDL 순서 1:1 반영)
# =========================================================
write_chunks("purchase_history",
             "(purchase_id, purchase_date, purchase_qty, unit_price, item_id, store_id)",
             purchase_rows)

write_chunks("sales_transaction",
             "(transaction_id, cancel_reason, idempotency_key, payment_method, status, total_amount, total_discount, transaction_time, store_id)",
             trans_rows)

write_chunks("sales_line_item",
             "(line_id, line_amount, quantity, unit_price, menu_id, transaction_id)",
             line_rows)

write_chunks("sales_daily_summary",
             "(id, store_id, summary_date, total_sales, transaction_count)",
             daily_rows)

write_chunks("sales_menu_daily_summary",
             "(id, summary_date, total_amount, total_quantity, menu_id, store_id)",
             menu_daily_rows)

write_chunks("inventory_snapshot",
             "(snapshot_id, created_at, snapshot_date, stock_qty, item_id, store_id)",
             snap_rows)

# =========================================================
# 14) PAYROLL (payroll_run / payroll_history)
# =========================================================
# store별 월별 payroll_run 1개 + employee별 payroll_history 생성
months_all = sorted({ym for (_, ym) in month_sales.keys()})

pr_rows = []
ph_rows = []

# (store_id, ym) -> run_id
payroll_run_id_map = {}

for sm in stores_meta:
    s_id = sm["store_id"]
    for ym in months_all:
        run_id = ids["payroll_run"]; ids["payroll_run"] += 1
        payroll_run_id_map[(s_id, ym)] = run_id

        # payroll_run DDL:
        # (run_id, calculated_at, created_at, finalized_at, payroll_month, source, status, updated_at, version, store_id)
        pr_rows.append(
            f"({run_id}, NOW(6), NOW(6), NULL, '{ym}', '{PAYROLL_RUN_SOURCE}', '{PAYROLL_RUN_STATUS}', NOW(6), 1, {s_id})"
        )

# payroll_history: (payroll_id, base_wage, created_at, deduction_type, deductions, gross_pay, net_pay, paid_at,
#                   payroll_month, status, updated_at, wage_type, work_days, work_minutes, employee_id, store_id)
# work_days는 shift_date 기준 distinct day 수를 정확히 세려면 별도 집계가 필요하지만,
# 더미에서는 minutes>0이면 days를 대략 minutes/480로 근사.
for (emp_id, s_id, ym), minutes in work_minutes_by_emp_month.items():
    payroll_id = ids["payroll_history"]; ids["payroll_history"] += 1
    wage = emp_wage.get(emp_id, 10000)
    wage_type = emp_wage_type.get(emp_id, "HOURLY")
    gross = int(round((minutes / 60.0) * wage))
    # 공제: 9% + 3.3% = 12.3% 근사
    deductions = int(round(gross * 0.123))
    net = max(0, gross - deductions)
    work_days = max(1, int(round(minutes / (8 * 60))))

    ph_rows.append(
        f"({payroll_id}, {int(wage)}, NOW(6), 'RATE_12_3', {deductions}, {gross}, {net}, NULL, "
        f"'{ym}', '{PAYROLL_HISTORY_STATUS}', NOW(6), '{wage_type}', {work_days}, {minutes}, {emp_id}, {s_id})"
    )

add_sql("INSERT INTO payroll_run (run_id, calculated_at, created_at, finalized_at, payroll_month, source, status, updated_at, version, store_id) VALUES")
add_sql(",\n".join(pr_rows) + ";")

write_chunks("payroll_history",
             "(payroll_id, base_wage, created_at, deduction_type, deductions, gross_pay, net_pay, paid_at, payroll_month, status, updated_at, wage_type, work_days, work_minutes, employee_id, store_id)",
             ph_rows)

# =========================================================
# 15) ML FEATURES (ml_store_month_features / ml_profit_forecast)
# =========================================================
# near_store_count: store_neighbor에서 radius=500 기준 카운트
near_count = defaultdict(int)
neighbor_map = defaultdict(list)  # store_id -> [neighbor_store_ids]
for row in sn_rows:
    # row format: (neighbor_store_id, radius_m, store_id, created_at, distance_m, updated_at)
    parts = row.strip("()").split(",")
    n_id = int(parts[0].strip())
    radius = int(parts[1].strip())
    s_id = int(parts[2].strip())
    if radius == 500:
        near_count[s_id] += 1
        neighbor_map[s_id].append(n_id)

# avg_menu_price, menu_count는 매장 menu 기준(월마다 동일)
store_menu_count = {sm["store_id"]: len(menu_map[sm["store_id"]]) for sm in stores_meta}
store_avg_menu_price = {
    sm["store_id"]: (sum(float(m["price"]) for m in menu_map[sm["store_id"]]) / max(1, len(menu_map[sm["store_id"]])))
    for sm in stores_meta
}

# area_avg_sales: 이웃 매장 월 매출 평균(없으면 자기매출)
# price_gap_rate: 내 avg_menu_price vs 이웃 avg_menu_price 평균 차이율
store_industry = {sm["store_id"]: sm["industry"] for sm in stores_meta}
store_gu = {sm["store_id"]: sm["gu"] for sm in stores_meta}

profit_by_store_month = {}

for (s_id, ym), sales in month_sales.items():
    cogs = month_cogs.get((s_id, ym), 0.0)
    labor = month_labor_gross.get((s_id, ym), 0.0)

    # profit 음수 방지(요청)
    profit = max(0.0, sales - cogs - labor)
    profit_by_store_month[(s_id, ym)] = profit

# y_next_profit: 다음달 profit (없으면 NULL)
y_next_profit = {}
for (s_id, ym), p in profit_by_store_month.items():
    y_next_profit[(s_id, ym)] = profit_by_store_month.get((s_id, next_ym(ym)))

# ml_store_month_features rows
ml_feat_rows = []
for (s_id, ym), sales in sorted(month_sales.items(), key=lambda x: (x[0][0], x[0][1])):
    gu = store_gu[s_id]
    industry = store_industry[s_id]

    cogs = month_cogs.get((s_id, ym), 0.0)
    labor = month_labor_gross.get((s_id, ym), 0.0)

    # rate 계산(0 division 방지)
    cogs_rate = (cogs / sales) if sales > 0 else 0.0
    labor_rate = (labor / sales) if sales > 0 else 0.0

    menu_count = store_menu_count[s_id]
    avg_menu_price = store_avg_menu_price[s_id]
    near_store_count = near_count.get(s_id, 0)

    neighbors = neighbor_map.get(s_id, [])
    if neighbors:
        neighbor_sales = [month_sales.get((nid, ym), 0.0) for nid in neighbors]
        area_avg_sales = sum(neighbor_sales) / max(1, len(neighbor_sales))
        neighbor_prices = [store_avg_menu_price.get(nid, avg_menu_price) for nid in neighbors]
        neighbor_avg_price = sum(neighbor_prices) / max(1, len(neighbor_prices))
    else:
        area_avg_sales = sales
        neighbor_avg_price = avg_menu_price

    price_gap_rate = ((avg_menu_price - neighbor_avg_price) / neighbor_avg_price) if neighbor_avg_price > 0 else 0.0

    profit = profit_by_store_month.get((s_id, ym), 0.0)
    profit_rate = (profit / sales) if sales > 0 else 0.0

    ynp = y_next_profit.get((s_id, ym), None)

    # DDL:
    # (id, store_id, ym, y_next_profit, sigungu_cd_nm, industry, sales_amount, cogs_rate, labor_amount, labor_rate,
    #  menu_count, avg_menu_price, near_store_count, area_avg_sales, price_gap_rate, profit_amount, profit_rate, created_at, updated_at)
    ml_id = ids["ml_feat"]; ids["ml_feat"] += 1
    ynp_sql = "NULL" if ynp is None else str(int(round(ynp)))

    ml_feat_rows.append(
        f"({ml_id}, {s_id}, '{ym}', {ynp_sql}, '{sql_escape(gu)}', '{sql_escape(industry)}', "
        f"{sales:.2f}, {cogs_rate:.4f}, {labor:.2f}, {labor_rate:.4f}, "
        f"{menu_count}, {avg_menu_price:.2f}, {near_store_count}, {area_avg_sales:.2f}, {price_gap_rate:.4f}, "
        f"{profit:.2f}, {profit_rate:.4f}, NOW(6), NOW(6))"
    )

write_chunks(
    "ml_store_month_features",
    "(id, store_id, ym, y_next_profit, sigungu_cd_nm, industry, sales_amount, cogs_rate, labor_amount, labor_rate, menu_count, avg_menu_price, near_store_count, area_avg_sales, price_gap_rate, profit_amount, profit_rate, created_at, updated_at)",
    ml_feat_rows
)

# ml_profit_forecast: feature_ym 기준으로 next month 예측 하나씩
ml_forecast_rows = []
for (s_id, ym), ynp in sorted(y_next_profit.items(), key=lambda x: (x[0][0], x[0][1])):
    if ynp is None:
        continue
    # pred = 실제 ynp에 노이즈 5~12% 부여
    noise = random.uniform(-0.08, 0.12)
    pred = int(round(max(0.0, ynp * (1.0 + noise))))

    f_id = ids["ml_forecast"]; ids["ml_forecast"] += 1
    pred_for = next_ym(ym)

    # DDL: (id, store_id, feature_ym, pred_for_ym, target, pred_value, model_path, created_at)
    ml_forecast_rows.append(
        f"({f_id}, {s_id}, '{ym}', '{pred_for}', 'y_next_profit', {pred}, 'models/xgb_store_profit_{s_id}.joblib', NOW(6))"
    )

write_chunks(
    "ml_profit_forecast",
    "(id, store_id, feature_ym, pred_for_ym, target, pred_value, model_path, created_at)",
    ml_forecast_rows
)

# =========================================================
# 16) FINAL SCRIPT
# =========================================================
final_script = [
    "SET NAMES utf8mb4;",
    "SET CHARACTER SET utf8mb4;",
    "SET character_set_connection = utf8mb4;",
    "-- Dummy Data Append Script (Gangnam/Jongno only, store.status=APPROVED, ML+Payroll+Neighbor+TradeArea 포함)",
]
final_script.extend(sql_statements)

with open("insert_data_final.sql", "w", encoding="utf-8") as f:
    f.write("\n\n".join(final_script))

print("Done.")
print(" - insert_data_final.sql created.")
print(" - weather_daily_features.csv created.")
