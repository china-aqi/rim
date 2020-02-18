from functools import lru_cache

import sqlalchemy
import pandas as pd


def get_securities():
    return pd.read_sql('securities', con=sqlalchemy.create_engine('sqlite:///../../data/jq.db'))


@lru_cache(maxsize=1)
def get_profit_forecast(today: str):
    assert today is not None    # 这个参数是为了cache需要，只要在一个日子中，就不需要重复从数据库拿数据
    df = pd.read_sql('profit_forecast', con=sqlalchemy.create_engine('sqlite:///../../data/em1.db'))\
        .set_index('code')\
        .drop('index', axis=1)
    return df[['eps_2019', 'eps_2020', 'eps_2021']].apply(pd.to_numeric, errors='coerce', downcast='float')


@lru_cache(maxsize=1)
def get_indicator2018(today: str):
    assert today is not None    # 这个参数是为了cache需要，只要在一个日子中，就不需要重复从数据库拿数据
    return pd.read_sql('indicator2018', con=sqlalchemy.create_engine('sqlite:///../../data/ts.db'))\
        .set_index('ts_code')\
        .drop('index', axis=1)


if __name__ == "__main__":
    test_today = '2020-02-11'
    df = get_profit_forecast(test_today)
    print(df)
