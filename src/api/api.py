import uvicorn
from fastapi import FastAPI

from src.stock_data import read_db as rdb
from src.business import security

app = FastAPI()


@app.get("/securities")
def read_securities():
    return {"hello world": security.get_securities(rdb.get_securities)}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
