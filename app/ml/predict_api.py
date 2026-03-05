# app/predict_api.py
import os
import joblib
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import date

from app.ml_features import fetch_features

app = FastAPI(title="ERP-AI Profit Forecast API")

TARGET = "y_next_profit"

MODEL_PATH = os.getenv("MODEL_PATH")
if not MODEL_PATH:
    # 서버 뜰 때 바로 알리려고 에러
    raise RuntimeError("MODEL_PATH env is required")

pipe = joblib.load(MODEL_PATH)

class ProfitForecastResponse(BaseModel):
    storeId: int
    year: int
    month: int
    featureYm: str
    predForYm: str
    target: str
    pred: int
    modelPath: str

@app.get("/predict/profit", response_model=ProfitForecastResponse)
def predict_profit(storeId: int, year: int, month: int):
    # featureYm = 요청한 (year-month)
    featureYm = f"{year:04d}-{month:02d}"

    # predForYm = 다음달
    if month == 12:
        predForYm = f"{year+1:04d}-01"
    else:
        predForYm = f"{year:04d}-{month+1:02d}"

    df = fetch_features(storeId, featureYm)
    if df.empty:
        raise HTTPException(
            status_code=400,
            detail={
                "code": "ML_FEATURE_NOT_READY",
                "message": f"예측에 필요한 데이터가 없습니다. (storeId={storeId}, ym={featureYm})",
                "details": None
            }
        )

    # 모델 입력 컬럼만
    X = df.drop(columns=["store_id", "ym"], errors="ignore")
    yhat = pipe.predict(X)[0]

    return ProfitForecastResponse(
        storeId=storeId,
        year=year,
        month=month,
        featureYm=featureYm,
        predForYm=predForYm,
        target=TARGET,
        pred=int(round(float(yhat))),
        modelPath=MODEL_PATH
    )
