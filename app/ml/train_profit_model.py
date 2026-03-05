# app/train_profit_model.py
import os
from datetime import datetime
import joblib
import pandas as pd
from sqlalchemy import create_engine
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error
from sklearn.ensemble import RandomForestRegressor

TARGET = "y_next_profit"

FEATURE_COLS_NUM = [
    "sales_amount", "cogs_rate", "labor_amount", "labor_rate",
    "menu_count", "avg_menu_price",
    "near_store_count", "area_avg_sales", "price_gap_rate",
]
FEATURE_COLS_CAT = ["sigungu_cd_nm", "industry"]

def get_engine():
    mysql_url = os.getenv("MYSQL_URL")
    if not mysql_url:
        raise RuntimeError("MYSQL_URL env is required")
    return create_engine(mysql_url, pool_pre_ping=True)

def load_df():
    sql = f"""
    SELECT
      store_id, ym,
      sigungu_cd_nm, industry,
      {", ".join(FEATURE_COLS_NUM)},
      {TARGET}
    FROM ml_store_month_features
    WHERE {TARGET} IS NOT NULL
    """
    df = pd.read_sql(sql, get_engine())
    return df

def main():
    df = load_df()
    if df.empty:
        raise RuntimeError("No training rows. y_next_profit is NULL for all rows.")

    # ✅ 아주 적은 데이터도 돌아가게: train/test 나누기 대신 전부 train + 간단 MAE만 출력
    X = df[FEATURE_COLS_CAT + FEATURE_COLS_NUM]
    y = df[TARGET].astype(float)

    pre = ColumnTransformer(
        transformers=[
            ("cat", Pipeline([
                ("imputer", SimpleImputer(strategy="most_frequent")),
                ("ohe", OneHotEncoder(handle_unknown="ignore")),
            ]), FEATURE_COLS_CAT),
            ("num", Pipeline([
                ("imputer", SimpleImputer(strategy="median")),
            ]), FEATURE_COLS_NUM),
        ]
    )

    model = RandomForestRegressor(
        n_estimators=400,
        random_state=42,
        n_jobs=-1
    )

    pipe = Pipeline(steps=[("pre", pre), ("model", model)])
    pipe.fit(X, y)

    # ✅ 훈련 데이터 기준 MAE (샘플 적을 때는 참고용)
    pred = pipe.predict(X)
    mae = mean_absolute_error(y, pred)
    print(f"[TRAIN] rows={len(df)} mae(train)={mae:.2f}")

    os.makedirs("models", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"models/profit_model_{TARGET}_{ts}.joblib"
    joblib.dump(pipe, path)
    print(f"[SAVE] {path}")

if __name__ == "__main__":
    main()
