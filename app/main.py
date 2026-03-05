import os
from datetime import datetime, timedelta
from fastapi import FastAPI
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
import holidays
import uvicorn

# 1. 환경 설정 및 앱 초기화
load_dotenv()
app = FastAPI(title="Intelligent ERP AI Server", version="3.1.0")

# DB 연결 정보 (사용자 제공 정보 반영)
DB_USER = os.getenv("DB_USER", "CSM")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234")
DB_HOST = os.getenv("DB_HOST", "192.168.4.37")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "erp")
DB_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DB_URL)

# 한국 공휴일 설정
kr_holidays = holidays.KR()

# 특수 이벤트 가중치
SPECIAL_EVENTS = {
    "2026-06-11": "WorldCup_Start",
    "2026-10-31": "Halloween",
    "2026-12-25": "Christmas"
}

def get_event_score(date_str):
    return 1.5 if date_str in SPECIAL_EVENTS else 1.0

# ---------------------------------------------------------
# [로직 1] 최근 오차 기반 가중치(Bias) 계산 함수
# ---------------------------------------------------------
def get_recent_prediction_bias():
    bias_map = {}
    query = text("""
        SELECT store_id, menu_id, 
               AVG(actual_qty) as avg_actual, 
               AVG(predicted_qty) as avg_pred
        FROM demand_forecast
        WHERE target_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
          AND actual_qty IS NOT NULL
        GROUP BY store_id, menu_id
    """)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(query).fetchall()
            for row in result:
                store_id, menu_id, avg_actual, avg_pred = row
                if avg_pred and avg_pred > 0.1:
                    bias = avg_actual / avg_pred
                    bias_map[(store_id, menu_id)] = max(0.5, min(2.0, bias))
                else:
                    bias_map[(store_id, menu_id)] = 1.0
        print(f"📊 최근 오차 분석 완료: {len(bias_map)}개 항목 보정 계수 적용 준비")
    except Exception as e:
        print(f"⚠️ Bias 계산 건너뜀 (신규 데이터셋 가능성): {e}")
    
    return bias_map

# ---------------------------------------------------------
# [로직 2] 모델 학습 함수
# ---------------------------------------------------------
def train_model():
    print("🚀 학습 데이터 조회 시작...")
    
    query_sales = """
        SELECT 
            s.summary_date as date, s.store_id, s.menu_id, s.total_quantity as qty,
            m.price, m.category_name, g.nx, g.ny
        FROM sales_menu_daily_summary s
        JOIN menu_item m ON s.menu_id = m.menu_id
        JOIN store_gps g ON s.store_id = g.store_id
        WHERE s.summary_date >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
    """
    
    query_weather = """
        SELECT forecast_date as date, nx, ny,
               AVG(temperature) as avg_temp, SUM(rainfall_mm) as daily_rain
        FROM weather_hourly
        WHERE forecast_date >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
        GROUP BY forecast_date, nx, ny
    """
    
    df_sales = pd.read_sql(query_sales, engine)
    df_weather = pd.read_sql(query_weather, engine)
    
    if df_sales.empty:
        return None, None, None, None

    # 데이터 병합 및 피처 엔지니어링
    df_train = pd.merge(df_sales, df_weather, on=['date', 'nx', 'ny'], how='left')
    df_train = df_train.fillna({'avg_temp': 15.0, 'daily_rain': 0.0})
    
    df_train['date'] = pd.to_datetime(df_train['date'])
    df_train['month'] = df_train['date'].dt.month
    df_train['weekday'] = df_train['date'].dt.weekday
    df_train['is_holiday'] = df_train['date'].apply(lambda x: 1 if x in kr_holidays else 0)
    df_train['event_score'] = df_train['date'].dt.strftime('%Y-%m-%d').apply(get_event_score)
    
    le = LabelEncoder()
    df_train['cat_code'] = le.fit_transform(df_train['category_name'])
    
    features = ['price', 'cat_code', 'avg_temp', 'daily_rain', 'month', 'weekday', 'is_holiday', 'event_score']
    X = df_train[features]
    y = df_train['qty']
    
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X, y)
    
    df_items = df_train[['store_id', 'menu_id', 'price', 'cat_code', 'nx', 'ny']].drop_duplicates()
    
    return model, le, df_items, features

# ---------------------------------------------------------
# [API] 수요 예측 및 DB 저장
# ---------------------------------------------------------
@app.post("/train")
def run_ai_prediction():
    # 1. 모델 학습 및 피처 정보 가져오기
    model, le, df_items, features_list = train_model()
    if model is None:
        return {"status": "error", "message": "No training data (check sales_menu_daily_summary table)"}
    
    bias_map = get_recent_prediction_bias()
    predictions = []
    today = datetime.now().date()
    
    print(f"🔮 향후 7일 예측 수행 중... (예측기준일: {today})")

    # 2. 미래 7일간 예측 루프
    for i in range(1, 8):
        target_date = today + timedelta(days=i)
        
        # 미래 날씨 데이터 조회
        weather_query = text("SELECT AVG(temperature), SUM(rainfall_mm) FROM weather_hourly WHERE forecast_date = :d")
        with engine.connect() as conn:
            w_res = conn.execute(weather_query, {"d": target_date}).fetchone()
            avg_temp = w_res[0] if w_res[0] is not None else 15.0
            daily_rain = w_res[1] if w_res[1] is not None else 0.0

        for _, item in df_items.iterrows():
            input_data = [
                item['price'], item['cat_code'], avg_temp, daily_rain,
                target_date.month, target_date.weekday(),
                1 if target_date in kr_holidays else 0,
                get_event_score(target_date.strftime('%Y-%m-%d'))
            ]
            
            # 경고 방지를 위해 DataFrame 형태로 예측 수행
            input_df = pd.DataFrame([input_data], columns=features_list)
            base_pred = model.predict(input_df)[0]
            
            # 가중치 적용 및 최종 수량 결정
            menu_bias = bias_map.get((item['store_id'], item['menu_id']), 1.0)
            final_pred_qty = max(0, int(round(base_pred * menu_bias)))
            
            # [수정] DB의 NOT NULL 컬럼(predicted_sales_max, predicted_visitors) 반영
            predictions.append({
                "store_id": int(item['store_id']),
                "menu_id": int(item['menu_id']),
                "forecast_date": today,                  # 예측 수행 날짜 (필수)
                "target_date": target_date,              # 예측 대상 날짜 (필수)
                "predicted_qty": final_pred_qty,         # 예상 수량
                "predicted_sales_max": float(final_pred_qty * item['price']), # 예상 매출액 (NOT NULL 대응)
                "predicted_visitors": 0,                 # 예상 방문자 (NOT NULL 대응, 기본값 0)
                "accuracy_rate": None,
                "actual_qty": None,
                "is_reflected": 0,                       # bit(1) 대응
                "created_at": datetime.now()
            })
    
    # 3. DB 저장 (삭제 후 삽입으로 중복 방지)
    if not predictions:
        return {"status": "error", "message": "Prediction failed"}

    df_to_save = pd.DataFrame(predictions)
    
    try:
        with engine.connect() as conn:
            # 중복 방지를 위해 오늘 이후의 예측 데이터 삭제
            conn.execute(text("DELETE FROM demand_forecast WHERE target_date >= :t"), {"t": today + timedelta(days=1)})
            conn.commit()
            
        # 데이터프레임을 DB에 인서트
        df_to_save.to_sql('demand_forecast', con=engine, if_exists='append', index=False)
        print(f"✅ 성공: {len(df_to_save)}건의 데이터가 demand_forecast 테이블에 저장되었습니다.")
        return {"status": "success", "count": len(df_to_save)}
        
    except Exception as e:
        print(f"❌ DB 저장 중 오류 발생: {e}")
        return {"status": "error", "message": str(e)}

# ---------------------------------------------------------
# [API] 실제치 업데이트 및 정확도 계산
# ---------------------------------------------------------
@app.post("/retrain")
def update_actual_data():
    try:
        sql_update = text("""
            UPDATE demand_forecast df
            JOIN sales_menu_daily_summary sales 
              ON df.store_id = sales.store_id 
              AND df.menu_id = sales.menu_id 
              AND df.target_date = sales.summary_date
            SET 
                df.actual_qty = sales.total_quantity,
                df.accuracy_rate = CASE 
                    WHEN sales.total_quantity = 0 THEN 0
                    ELSE (1 - ABS(df.predicted_qty - sales.total_quantity) / GREATEST(sales.total_quantity, 1)) * 100
                END,
                df.is_reflected = 1
            WHERE df.actual_qty IS NULL AND sales.summary_date < CURDATE()
        """)
        
        with engine.connect() as conn:
            result = conn.execute(sql_update)
            conn.commit()
            
        return {"status": "success", "updated_count": result.rowcount}
    except Exception as e:
        print(f"❌ Retrain 오류: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)