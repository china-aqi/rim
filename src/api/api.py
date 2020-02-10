import uvicorn
from fastapi import FastAPI

from src.stock_data import read_db as rdb
from src.business import security, rim

app = FastAPI()


@app.get("/securities")
def read_securities():
    return {"hello world": security.get_securities(rdb.get_securities)}


@app.get("/profit-forecast")
def read_profit_forecast():
    t = rim.get_profit_forecast(rdb.get_profit_forecast)
    return {"profit forecast": rim.get_profit_forecast(rdb.get_profit_forecast)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
