from typing import Callable
import datetime

import pandas as pd

from src.stock_data import read_db as rdb


def get_profit_forecast(code: str, getter: Callable[[str], pd.DataFrame]):
    """ 获取分析师的盈利预期
    """
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    return getter(today).loc[code].to_dict()


if __name__ == "__main__":
    print(get_profit_forecast('000625', rdb.get_profit_forecast))
