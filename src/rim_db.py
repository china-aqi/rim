from functools import lru_cache
import datetime
from typing import Tuple, List

import sqlalchemy
import pandas as pd


def get_securities():
    return pd.read_sql('securities', con=sqlalchemy.create_engine('sqlite:///../data/jq.db'))


@lru_cache(maxsize=1)
def get_profit_forecast(today: str):
    assert today is not None    # 这个参数是为了cache需要，只要在一个日子中，就不需要重复从数据库拿数据
    df = pd.read_sql('profit_forecast', con=sqlalchemy.create_engine('sqlite:///../data/em1.db'))\
        .set_index('code')\
        .drop('index', axis=1)
    return df[['eps_2019', 'eps_2020', 'eps_2021']].apply(pd.to_numeric, errors='coerce', downcast='float')


@lru_cache(maxsize=1)
def get_indicator(year: str = '2018'):
    assert year == '2018'
    indicator: pd.DataFrame = pd.read_sql('indicator2018', con=sqlalchemy.create_engine('sqlite:///../data/ts.db'))
    indicator['ts_code'] = indicator['ts_code'].map(lambda x: x[:6])
    return indicator.set_index('ts_code')\
        .drop('index', axis=1)


def _today() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d")


@lru_cache(maxsize=1)
def get_financial_indicator(today: str = _today()) -> pd.DataFrame:
    """
    get the tushare financial indicator from ts.db
    That is a table with ts_code, end_date and grossprofit_margin column

    Parameters
    ----------
    today : str
        today这个参数是为了cache需要，只要在一个日子中，就不需要重复从数据库拿数据

    Returns
    -------
    table : DataFrame
        剔除了毛利率异常的数据（grossprofit_margin<=0 or grossprofit_margin>=100)
        按ts_code、end_date字典序排序
    """
    return pd.read_sql('SELECT ts_code, end_date, grossprofit_margin FROM financial_indicator \
                        WHERE 0 <= grossprofit_margin and grossprofit_margin <= 100\
                        ORDER BY ts_code, end_date',
                       con=sqlalchemy.create_engine('sqlite:///../../data/ts.db'))\
        .set_index(['ts_code', 'end_date'])


@lru_cache(maxsize=1)
def get_ts_statement(name: str, today: str = _today()) -> pd.DataFrame:
    """
    get the statement from ts.db

    Parameters
    ----------
    name: str
        statement name, for example, 'balancesheet'
    today : str
        today这个参数是为了cache需要，只要在一个日子中，就不需要重复从数据库拿数据

    Returns
    -------
    table : DataFrame
    """
    return pd.read_sql(f'SELECT * FROM {name}',
                       con=sqlalchemy.create_engine('sqlite:///../../data/ts.db'))\
        .set_index(['ts_code', 'end_date'])


def get_financial_indicator_by_code(code: str) -> pd.DataFrame:
    """ 获取某个公司最近数年的财务指标
    输入假设：
    code 符合tushare要求的上市公司代码
    输出规定:
    列名同tushare的财务指标表格，包含了code的所有数据
    """
    return get_financial_indicator().loc[code]


def save_profitability_index_to_db(data: List[Tuple[str, float, int, float, int]]) -> None:
    """
    把各个公司代码、盈利增长指标、盈利增长百分位、盈利稳定指标、盈利稳定百分位等信息保存到数据库

    :param data: list of tuple
                元组中第一项为公司代码，以后依次为ms, ms rank，mg，mg rank

    :return: None
    """
    p = pd.DataFrame(data, columns=['ts_code', 'mg', 'mg_rank', 'ms', 'ms_rank']).set_index('ts_code')
    p.to_sql('profitability_index', con=sqlalchemy.create_engine('sqlite:///../../data/indicator.db'),
             if_exists='replace')


@lru_cache(maxsize=1)
def read_profitability_index(today: str = _today()) -> pd.DataFrame:
    """
    从indicator数据库中读取盈利能力指标，包括了盈利增长指标和其全市场百分位，盈利稳定性指标和其全市场百分位

    :param today: 日期字符串
                    此参数主要是为了cache，一般盈利能力指标在日内是不会变化的
    :return: index为ts_code的DataFrame，有4各栏位mg, mg_rank, ms and ms_rank
    """
    return pd.read_sql('profitability_index', con=sqlalchemy.create_engine('sqlite:///../../data/indicator.db'))\
        .set_index('ts_code')


if __name__ == "__main__":
    df = get_financial_indicator()
    print(df)
