from typing import List, NoReturn, Tuple, Iterator
import sched, time
from itertools import groupby
import datetime

import pandas as pd
import sqlalchemy
import tushare as ts

from src import config

ts.set_token(config.ts_token)


def tushare_indicator_to_db(index: int, codes: List[str]) -> NoReturn:
    indicators: List[dict] = []
    print(f"第{index}批次 {datetime.datetime.now()}")
    for code in codes:
        print(f"{code}")
        indicator: pd.DataFrame = pro.fina_indicator(ts_code=code, period='20181231', fields='ts_code, eps, bps')
        if indicator.empty is False:
            indicators.append(indicator.iloc[0].to_dict())
    pd.DataFrame(indicators).to_sql('indicator2018', con=sqlalchemy.create_engine('sqlite:///../../data/ts.db'),
                                    if_exists= 'replace' if index == 0 else 'append', chunksize=1024)


if __name__ == '__main__':
    pro = ts.pro_api()
    s = sched.scheduler(time.time, time.sleep)
    securities: pd.DataFrame = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
    securities_list: List[Tuple[int, str]] = [(t.Index, t.ts_code) for t in securities.itertuples()]
    jobs: Iterator = groupby(securities_list, key=lambda x: x[0]//70)
    for i, items in jobs:
        s.enter(i * 60, 1, tushare_indicator_to_db, kwargs={'codes': [item[1] for item in items], 'index': i})
    s.run()
