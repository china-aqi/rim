import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from src.stock_data import read_db as rdb
from src.business import security, rim

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8001",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/securities")
def read_securities():
    return {"hello world": security.get_securities(rdb.get_securities)}


@app.get("/profit-forecast/")
def read_profit_forecast(code: str):
    return {f"{code} profit forecast": rim.get_profit_forecast(code, rdb.get_profit_forecast)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
