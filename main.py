import uvicorn
from fastapi import FastAPI

from src.business import security
from src.stock_data import read_db

app = FastAPI()


@app.get("/securities")
def read_securities():
    l = security.get_securities(read_db.get_securities)
    # return {"securities": security.get_securities(read_db.get_securities)}
    return {"h": 'Hello'}


if __name__ == "__main__":
    l = security.get_securities(read_db.get_securities)
    uvicorn.run(app, host="127.0.0.1", port=8000)
