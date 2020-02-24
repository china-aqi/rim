from functools import lru_cache
import time
import datetime

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


def _today() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d")


@lru_cache(maxsize=1)
def get_financial_indicator(today: str = _today()):
    assert today is not None    # 这个参数是为了cache需要，只要在一个日子中，就不需要重复从数据库拿数据
    return pd.read_sql('financial_indicator', con=sqlalchemy.create_engine('sqlite:///../../data/ts.db'))\
        .set_index(['ts_code', 'end_date'])\
        .drop('index', axis=1)


def get_financial_indicator_by_code(code: str) -> pd.DataFrame:
    """ 获取某个公司最近数年的财务指标
    输入假设：
    code 符合tushare要求的上市公司代码
    输出规定:
    列名同tushare的财务指标表格，包含了code的所有数据
    """
    return get_financial_indicator().loc[code]


if __name__ == "__main__":
    df = get_financial_indicator_by_code('000625.SZ')
    print(df)
