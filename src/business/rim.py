from typing import Callable

import pandas as pd

from src.stock_data import read_db as rdb


def get_profit_forecast(getter: Callable[[], pd.DataFrame]):
    """ 获取分析师的盈利预期
    """
    return [{'code': t.code, 'number_of_reports': t.number_of_reports, 'eps2018': t.eps_2018, 'eps2019': t.eps_2019,
             'eps2020': t.eps_2020, 'eps2021': t.eps_2021, 'rank_buy': t.rank_buy, 'rank_increase': t.rank_increase,
             'rank_neutral': t.rank_neutral, 'rank_reduction': t.rank_reduction}
            for t in getter().itertuples(index=False)]


if __name__ == "__main__":
    print(get_profit_forecast(rdb.get_profit_forecast))
