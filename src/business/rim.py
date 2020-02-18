from typing import Callable, List
import datetime
from itertools import product

import pandas as pd

from src.stock_data import read_db as rdb


def get_profit_forecast(code: str,
                        getter: Callable[[str], pd.DataFrame] = rdb.get_profit_forecast):
    """ 获取分析师的盈利预期
    """
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    return getter(today).loc[code].to_dict()


def _convert_to_ts_code(code: str) -> str:
    return code + '.SH' if code[0] == '6' else code + '.SZ'


def get_indicator2018(code: str,
                      getter: Callable[[str], pd.DataFrame] = rdb.get_indicator2018) -> dict:
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    ts_code = _convert_to_ts_code(code)
    return getter(today).loc[ts_code].to_dict()


def calculate_rim_value(code: str) -> dict:
    def cal_re(bps_0: float, forecast_eps: List[float], rr: float) -> List[float]:
        # 计算各期的剩余收益
        re_lst: List[float] = []
        bps: float = bps_0
        for eps in forecast_eps:
            re_lst.append(eps - rr * bps)
            bps = bps + eps
        return re_lst

    def discounted(amount_lst: List[float], discount_exponents: List[float], rr) -> List[float]:
        """ 计算各期金额的折现值
        """
        assert len(amount_lst) == len(discount_exponents)
        return [amount_lst[i] / (1+rr) ** discount_exponents[i] for i in range(len(amount_lst))]

    def cal_cv(re: float, rr: float, gr: float) -> float:
        """ 计算第t期后持续期的剩余收益"""
        assert rr > gr
        return re * (1 + gr) / (rr - gr)

    def cal_rim_value(rr: float, gr: float):
        re = cal_re(indicator2018['bps'], [profit_forecast['eps_2019'],
                                           profit_forecast['eps_2020'],
                                           profit_forecast['eps_2021']], rr)
        cv = cal_cv(re[2], rr, gr)
        re.append(cv)
        discounted_re = discounted(re, [0, 0.85, 1.85, 1.85], rr)
        value = indicator2018['bps'] + sum(discounted_re)
        return {
            'rr': rr,
            'gr': gr,
            'value': value,
            'discounted_re2019': discounted_re[0],
            'discounted_re2020': discounted_re[1],
            'discounted_re2021': discounted_re[2],
            'discounted_cv': discounted_re[3],
        }

    profit_forecast = get_profit_forecast(code)
    indicator2018 = get_indicator2018(code)

    rr_lst: List[float] = [0.08, 0.09, 0.10, 0.11, 0.12]
    gr_lst: List[float] = [0.0, 0.02, 0.04]
    rr_gr_lst = product(rr_lst, gr_lst)
    rim_values = [cal_rim_value(rr, gr)for rr, gr in rr_gr_lst]

    return {
        'bps2018': indicator2018['bps'],
        're': rim_values,
        'rr': rr_lst,
        'gr': gr_lst
    }


if __name__ == "__main__":
    print(calculate_rim_value('000625'))
