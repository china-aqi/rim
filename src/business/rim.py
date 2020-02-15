from typing import Callable
import datetime

import pandas as pd

from src.stock_data import read_db as rdb


def get_profit_forecast(code: str, getter: Callable[[str], pd.DataFrame]):
    """ 获取分析师的盈利预期
    """
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    return getter(today).loc[code].to_dict()


def _convert_to_ts_code(code: str) -> str:
    return code + '.SH' if code[0] == '6' else code + '.SZ'


def get_indicator2018(code: str, getter: Callable[[str], pd.DataFrame]) -> dict:
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    ts_code = _convert_to_ts_code(code)
    return getter(today).loc[ts_code].to_dict()


if __name__ == "__main__":
    print(get_indicator2018('000625', rdb.get_indicator2018))
