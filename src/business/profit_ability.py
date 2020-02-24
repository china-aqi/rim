""" 盈利能力指标
"""
from typing import Callable, Tuple, List, Iterator, Optional
from itertools import takewhile

from toolz import pipe
import numpy as np
import pandas as pd
from scipy.stats import gmean

from src.stock_data import read_db as rdb


def _convert_to_ts_code(code: str) -> str:
    return code + '.SH' if code[0] == '6' else code + '.SZ'


def calculate_yrs_roe(code: str,
                      getter: Callable[[str], pd.DataFrame] = rdb.get_financial_indicator) -> Tuple[List[str], float]:
    """ 返回上市公司ROE的最近若干年的几何平均数
    输入假设：
    code 是符合tushare规定的上市公司代码
    getter 是返回tushare财务指标（Dataframe格式）的函数，其中有ts_code, roe, end_date column

    输出规定：
    最近4~8年ROE的几何平均数，例如，(['2018', '2017', '2016', '2015', '2014', '2013'], 0.098)
    其中元组的第一项保存参与（几何平均）运算的年份-ROE序列，第二项是平均roe
    从最近的公布的年财务指标开始连续拿数据，最多8年数据，最少4年数据。若数据不足，直接返回None。
    """
    roe_it = getter(code).loc[_convert_to_ts_code(code)]['roe'].sort_index().iteritems()
    roe_lst = [roe for roe in roe_it]
    last_roe_it = takewhile(lambda x: not np.isnan(x[1]), reversed(roe_lst))
    last_year_roe = [y_r for y_r in last_roe_it]
    len_year_roe = len(last_year_roe)
    last_year_roe = last_year_roe[0:8] if len_year_roe > 8 else last_year_roe
    return {'year_roe': last_year_roe, 'gmean_roe': gmean([roe for year, roe in last_year_roe])} \
        if len_year_roe >= 4 else None


def calculate_yrs_profitability(code: str,
                                getter: Callable[[str], pd.DataFrame] = rdb.get_financial_indicator) \
        -> Tuple[List[str], float]:
    """ 返回上市公司ROE的最近若干年的几何平均数
    输入假设：
    code 是符合tushare规定的上市公司代码
    getter 是返回tushare财务指标（Dataframe格式）的函数，其中有ts_code, roe, end_date column

    输出规定：
    最近4~8年ROE的几何平均数，例如，(['2018', '2017', '2016', '2015', '2014', '2013'], 0.098)
    其中元组的第一项保存参与（几何平均）运算的年份-ROE序列，第二项是平均roe
    从最近的公布的年财务指标开始连续拿数据，最多8年数据，最少4年数据。若数据不足，直接返回None。
    """
    roe_it = getter(code).loc[_convert_to_ts_code(code)]['roe'].sort_index().iteritems()
    roe_lst = [roe for roe in roe_it]
    last_roe_it = takewhile(lambda x: not np.isnan(x[1]), reversed(roe_lst))
    last_year_roe = [y_r for y_r in last_roe_it]
    len_year_roe = len(last_year_roe)
    last_year_roe = last_year_roe[0:8] if len_year_roe > 8 else last_year_roe
    return {'year_roe': last_year_roe, 'gmean_roe': gmean([roe for year, roe in last_year_roe])} \
        if len_year_roe >= 4 else None


def _get_yrs_gm(financial_indicators: pd.DataFrame) -> Iterator[Tuple[str, float, float]]:
    """ 获取最近数年的毛利数据
    输入假设：
    financial_indicators 是tushare的财务指标表，但仅包含某个公司的数据

    输出规定：
    Iterator, 其中内容例如，('2018', 0.24)
    若数据包含np.nan，则停止迭代，例如：('2019', 0.24), ('2018', 0.32) instead of
        ('2019', 0.24), ('2018', 0.32), ('2017', nan)
    """
    gm_it = financial_indicators['grossprofit_margin'].sort_index().iteritems()

    gm_lst = [(gm[0][:4], gm[1] / 100) for gm in gm_it]
    return takewhile(lambda x: not np.isnan(x[1]), reversed(gm_lst))


def _get_9yrs_gm(years_gm_it: Iterator[Tuple[str, float]]) -> Optional[List[Tuple[str, float]]]:
    """ 获取最近9年的毛利数据
    输入假设：
    years_gm Iterator, 其中内容例如，('2018', 0.24)，出现顺序是按年份的倒序

    输出规定：
    [('2018', 12018.0),('2017', 12017.0),...,('2010', 12010.0)]
    此列表最长9项，若输入数据不足9个年份，则按实际年份；若不足4个年份，则直接返回None
    """
    years_gm_lst = [ygm for ygm in years_gm_it]
    len_years_gm = len(years_gm_lst)
    years_gm_lst = years_gm_lst[0:9] if len_years_gm > 9 else years_gm_lst
    return years_gm_lst if len_years_gm > 4 else None


def _sort_9yrs_gm(years_gm_lst: Optional[List[Tuple[str, float]]]) -> Optional[List[Tuple[str, float]]]:
    """ 对最近9年的毛利按年份升序排序
    输入假设：
    years_gm_lst 列表，例如，[('2018', 0.18)，('2017', 0.17), ..., ('2010', 0.10)]。注意：可能为None

    输出假设：
    [('2010', 0.10)，('2011', 0.11), ..., ('2018', 0.18)]。注意：可能为None
    """
    return list(reversed(years_gm_lst)) if years_gm_lst is not None else None


def _calc_8yrs_mg(yrs_gm_lst: Optional[List[Tuple[str, float]]]) -> Optional[List[Tuple[str, float]]]:
    """ 计算最近8年（或数年）的毛利增长率
    输入假设：
    列表，例如，[('2010', 0.174433), ('2011', 0.147854), ..., ('2018', 0.146532)]。注意，可能为None
    ('2010', 0.174433)中的'2010'为年份，0.174433为此年份的毛利

    输出假设：
    列表，例如，[('2011', -0.1523), ('2012', 0.2447), ..., ('2018', 0.1009)]。注意，可能为None
    ('2011', 0.174433)中的'2011'为年份，-0.1523为此年份的毛利
    """
    def cal_gm_growth_rate(i: int) -> float:
        return (yrs_gm_lst[i][1] - yrs_gm_lst[i-1][1]) / yrs_gm_lst[i-1][1]

    return [(yrs_gm_lst[i][0], cal_gm_growth_rate(i)) for i in range(1, len(yrs_gm_lst))] \
        if yrs_gm_lst is not None else None


def _calc_gmean_8yrs_mg(yrs_mg_lst: Optional[List[Tuple[str, float]]]) \
        -> Optional[Tuple[float, List[Tuple[str, float]]]]:
    """ 计算最近8年（或数年）的毛利增长率的几何平均数
    输入假设：
    列表，例如，[('2011', -0.1523), ('2012', 0.2447), ..., ('2018', 0.1009)]。注意，可能为None
    ('2011', 0.174433)中的'2011'为年份，-0.1523为此年份的毛利

    输出假设：
    列表，例如，(-0.02, [('2011', -0.1523), ('2012', 0.2447), ..., ('2018', 0.1009)])。注意，可能为None
    其中-0.02是增长率的几何平均数
    """
    return (gmean([1+mg for year, mg in yrs_mg_lst]) - 1, yrs_mg_lst) if yrs_mg_lst is not None else None


def get_8yrs_mg(code: str,
                getter: Callable[[str], pd.DataFrame] = rdb.get_financial_indicator_by_code) -> Optional[dict]:
    """ 获取上市公司8年（或多年）的毛利增长率的几何平均数，并以dict返回。
    输入假设:
    列表，例如，(-0.02, [('2011', -0.1523), ('2012', 0.2447), ..., ('2018', 0.1009)])。注意，可能为None
    其中-0.02是增长率的几何平均数

    输出规定：
    {'mgr_gmean': -0.02, 'years_mgr': [('2011', -0.1523), ('2012', 0.2447), ..., ('2018', 0.1009)]}
    """
    mg = pipe(rdb.get_financial_indicator_by_code(_convert_to_ts_code(code)),
              _get_yrs_gm,
              _get_9yrs_gm,
              _sort_9yrs_gm,
              _calc_8yrs_mg,
              _calc_gmean_8yrs_mg)
    return {'mgr_gmean': mg[0], 'years_mgr': mg[1]} if mg is not None else None


if __name__ == "__main__":
    d = get_8yrs_mg('300072')
    print(d)
