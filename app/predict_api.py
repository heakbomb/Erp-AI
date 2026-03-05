import os
import joblib
from fastapi import FastAPI, HTTPException, Query

from app.ml_features import fetch_features

MODEL_PATH = os.getenv("MODEL_PATH")
if not MODEL_PATH:
    raise RuntimeError("MODEL_PATH env is required")

TARGET = os.getenv("ML_TARGET", "y_next_profit")

# ✅ uvicorn이 찾는 전역 변수 이름은 'app'
app = FastAPI(title="ERP AI Predict API", version="1.0.0")

# ✅ 서버 시작 시 1번 로드
pipe = joblib.load(MODEL_PATH)


def ym_str(y: int, m: int) -> str:
    if y < 2000 or y > 2100:
        raise HTTPException(status_code=400, detail="year must be 2000~2100")
    if m < 1 or m > 12:
        raise HTTPException(status_code=400, detail="month must be 1~12")
    return f"{y:04d}-{m:02d}"


def next_ym(y: int, m: int) -> str:
    if m == 12:
        return f"{y+1:04d}-01"
    return f"{y:04d}-{m+1:02d}"


@app.get("/predict/profit")
def predict_profit(
    storeId: int = Query(..., ge=1),
    year: int = Query(...),
    month: int = Query(...),
):
    featureYm = ym_str(year, month)
    predForYm = next_ym(year, month)

    df = fetch_features(storeId, featureYm)
    if df.empty:
        raise HTTPException(
            status_code=404,
            detail=f"Features not found for storeId={storeId}, ym={featureYm}. "
                   f"Create ml_store_month_features row first."
        )

    pred = float(pipe.predict(df)[0])

    return {
        "storeId": storeId,
        "year": year,
        "month": month,
        "featureYm": featureYm,
        "predForYm": predForYm,
        "target": TARGET,
        "pred": pred,
        "modelPath": MODEL_PATH,
    }

