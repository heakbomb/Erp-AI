# app/train_profit_model.py
import os
import json
from datetime import datetime
import pandas as pd

from sqlalchemy import create_engine
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from xgboost import XGBRegressor
import joblib


MYSQL_URL = os.getenv("MYSQL_URL")
if not MYSQL_URL:
    raise RuntimeError("MYSQL_URL env is required")

TABLE = os.getenv("ML_TABLE", "ml_store_month_features")
TARGET = os.getenv("ML_TARGET", "y_next_profit")  # y_next_profit or y_next_profit_rate
MODEL_DIR = os.getenv("MODEL_DIR", "models")
TEST_MONTHS = int(os.getenv("TEST_MONTHS", "2"))
MIN_SALES = float(os.getenv("MIN_SALES", "1"))


def load_data() -> pd.DataFrame:
    engine = create_engine(MYSQL_URL, pool_pre_ping=True)

    sql = f"""
        SELECT
         store_id, ym, sigungu_cd_nm, industry,
         sales_amount, labor_amount, labor_rate,
         menu_count, avg_menu_price,
         price_gap_rate, near_store_count, area_avg_sales,
         cogs_rate, profit_amount, profit_rate,
         y_next_profit, y_next_profit_rate
         FROM ml_store_month_features
         WHERE y_next_profit IS NOT NULL
    """
    df = pd.read_sql(sql, engine)

    df["ym"] = df["ym"].astype(str)
    df["sigungu_cd_nm"] = df["sigungu_cd_nm"].astype("string")
    df["industry"] = df["industry"].astype("string")

    df = df[df["sales_amount"].fillna(0) >= MIN_SALES].copy()

    num_cols = [
        "sales_amount", "labor_amount", "labor_rate",
        "menu_count", "avg_menu_price",
        "price_gap_rate", "near_store_count", "area_avg_sales",
        "cogs_rate", "profit_amount", "profit_rate",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    df["sigungu_cd_nm"] = df["sigungu_cd_nm"].fillna("UNKNOWN")
    df["industry"] = df["industry"].fillna("UNKNOWN")

    return df


def safe_split(df: pd.DataFrame, test_months: int):
    """
    데이터가 적을 때도 학습이 돌아가게 split 전략을 자동 결정
    - months >= 3: time split (최근 N개월 test)
    - months == 2: test_months를 1로 강제
    - months == 1: random split (최소 학습용)
    """
    months = sorted(df["ym"].unique())
    mcount = len(months)

    if mcount >= 3:
        n = min(test_months, mcount - 1)
        test_set = set(months[-n:])
        train_df = df[~df["ym"].isin(test_set)].copy()
        test_df = df[df["ym"].isin(test_set)].copy()
        return train_df, test_df, {"mode": "time", "test_months": n, "test_month_list": sorted(test_set)}

    if mcount == 2:
        test_set = {months[-1]}
        train_df = df[df["ym"] == months[0]].copy()
        test_df = df[df["ym"] == months[1]].copy()
        return train_df, test_df, {"mode": "time", "test_months": 1, "test_month_list": [months[1]]}

    # mcount == 1
    # 행이 너무 적으면 split도 어려움
    if len(df) < 4:
        raise RuntimeError(f"Not enough rows to train. rows={len(df)} months={months}. "
                           f"Need more months or more stores.")

    train_df, test_df = train_test_split(df, test_size=0.3, random_state=42)
    return train_df.copy(), test_df.copy(), {"mode": "random", "test_size": 0.3, "months": months}


def build_pipeline(cat_cols, num_cols) -> Pipeline:
    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", "passthrough", num_cols),
        ],
        remainder="drop",
    )

    model = XGBRegressor(
        n_estimators=400,
        max_depth=5,
        learning_rate=0.06,
        subsample=0.9,
        colsample_bytree=0.9,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
    )

    return Pipeline(steps=[
        ("preprocess", pre),
        ("model", model),
    ])


def evaluate(y_true, y_pred, label: str):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = mean_squared_error(y_true, y_pred) ** 0.5
    r2 = r2_score(y_true, y_pred)
    print(f"[{label}] MAE={mae:,.2f} | RMSE={rmse:,.2f} | R2={r2:.4f}")


def main():
    print(f"[LOAD] table={TABLE} target={TARGET}")
    df = load_data()
    print(f"[DATA] rows={len(df):,} months={df['ym'].nunique()} stores={df['store_id'].nunique()}")

    split_train, split_test, split_meta = safe_split(df, TEST_MONTHS)
    print(f"[SPLIT] {split_meta} train_rows={len(split_train):,} test_rows={len(split_test):,}")

    cat_cols = ["sigungu_cd_nm", "industry"]
    num_cols = [
        "sales_amount", "labor_amount", "labor_rate",
        "menu_count", "avg_menu_price",
        "price_gap_rate", "near_store_count", "area_avg_sales",
        "cogs_rate", "profit_amount", "profit_rate",
    ]
    num_cols = [c for c in num_cols if c in df.columns]

    X_train = split_train[cat_cols + num_cols]
    y_train = split_train[TARGET].astype(float)

    X_test = split_test[cat_cols + num_cols]
    y_test = split_test[TARGET].astype(float)

    pipe = build_pipeline(cat_cols, num_cols)

    print("[TRAIN] fitting...")
    pipe.fit(X_train, y_train)

    pred_train = pipe.predict(X_train)
    pred_test = pipe.predict(X_test)

    evaluate(y_train, pred_train, "TRAIN")
    evaluate(y_test, pred_test, "TEST")

    os.makedirs(MODEL_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = os.path.join(MODEL_DIR, f"profit_model_{TARGET}_{ts}.joblib")
    meta_path = os.path.join(MODEL_DIR, f"profit_model_{TARGET}_{ts}.meta.json")

    joblib.dump(pipe, model_path)

    meta = {
        "table": TABLE,
        "target": TARGET,
        "rows_total": int(len(df)),
        "rows_train": int(len(split_train)),
        "rows_test": int(len(split_test)),
        "split": split_meta,
        "cat_cols": cat_cols,
        "num_cols": num_cols,
        "created_at": ts,
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[SAVED] {model_path}")
    print(f"[SAVED] {meta_path}")


if __name__ == "__main__":
    main()
