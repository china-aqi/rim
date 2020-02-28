from typing import List

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware

from src.stock_data import rim_db as rdb
from src.business import security, rim, profit_ability

app = FastAPI()

# 允许跨域
origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
]

# 允许跨域
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


@app.get("/financial-indicator/")
def read_indicator2018(code: str):
    return {f"{code} 2018 financial indicator": rim.get_indicator2018(code, rdb.get_indicator2018)}


class RE(BaseModel):
    rr: float                       # required return, 必要投资报酬率 / 折现率
    gr: float                       # growth rate，持续期的剩余收益增长率
    value: float                    # 对企业的剩余收益估值
    discounted_re2019: float        # 折现后的2019年剩余收益
    discounted_re2020: float        # 折现后的2020年剩余收益
    discounted_re2021: float        # 折现后的2021年剩余收益
    discounted_cv: float            # # 折现后的持续期剩余收益


class RIMValue(BaseModel):
    bps2018: float                  # 2018年每股净资产
    rr: List[float]
    gr: List[float]
    re: List[RE]                    # 不同假设下的剩余收益


@app.get("/rim-value/", response_model=RIMValue)
def read_rim_value(code: str):
    return rim.calculate_rim_value(code)


@app.get("/profitability/8yr-roe/")
def read_years_roe(code: str):
    return profit_ability.calculate_yrs_roe(code)


class MGMSValue(BaseModel):
    code: str
    mm: int                 # maximum margin, MM = max(mg rank, ms rank)，最大盈利能力指标
    mg: float               # margin growth, MG = (II(1 + ΔGM / GM)) ^ 1/T - 1，盈利能力成长性指标
    mg_rank: int            # rank of mg, 盈利能力成长性在全市场的百分位
    ms: float               # margin stability, MS = AVG(GM) / STD(GM), 盈利能力稳定性指标
    ms_rank: int            # rank of ms, 盈利能力稳定性在全市场的百分位


@app.get("/profitability/mg-ms", response_model=MGMSValue)
def read_years_roe(code: str):
    return profit_ability.get_mg_ms(code)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)
