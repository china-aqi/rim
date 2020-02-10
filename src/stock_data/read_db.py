import sqlalchemy
import pandas as pd


def get_securities():
    return pd.read_sql('securities', con=sqlalchemy.create_engine('sqlite:///../../data/jq.db'))


def get_profit_forecast():
    return pd.read_sql('profit_forecast', con=sqlalchemy.create_engine('sqlite:///../../data/em1.db'))


if __name__ == "__main__":
    df = get_profit_forecast()
    print(df)
