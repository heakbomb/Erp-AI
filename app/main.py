from fastapi import FastAPI

app = FastAPI(title="ERP AI Server")

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/")
def root():
    return {"message": "AI server running"}
