""" 盈利能力指标
"""
from typing import Callable, Tuple, List, Iterator, Optional, Dict
from functools import partial, reduce
from statistics import geometric_mean, mean, stdev, quantiles
from itertools import tee

from toolz import pipe, juxt, compose
import pandas as pd

from src.stock_data import rim_db as rdb
from src.stock_data import crawl_tushare as cts


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
    # last_roe_it = takewhile(lambda x: not np.isnan(x[1]), reversed(roe_lst))
    # last_year_roe = [y_r for y_r in last_roe_it]
    # len_year_roe = len(last_year_roe)
    # last_year_roe = last_year_roe[0:8] if len_year_roe > 8 else last_year_roe
    # return {'year_roe': last_year_roe, 'gmean_roe': gmean([roe for year, roe in last_year_roe])} \
    #     if len_year_roe >= 4 else None


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
    # last_roe_it = takewhile(lambda x: not np.isnan(x[1]), reversed(roe_lst))
    # last_year_roe = [y_r for y_r in last_roe_it]
    # len_year_roe = len(last_year_roe)
    # last_year_roe = last_year_roe[0:8] if len_year_roe > 8 else last_year_roe
    # return {'year_roe': last_year_roe, 'gmean_roe': gmean([roe for year, roe in last_year_roe])} \
    #     if len_year_roe >= 4 else None


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
    # return takewhile(lambda x: not np.isnan(x[1]), reversed(gm_lst))


def _get_last_n_years_fi(it: Iterator[Tuple[str, float]],
                         max_years: int = 8,
                         min_years: int = 4) -> Optional[List[Tuple[str, float]]]:
    """ 获取最近n年的财务指标数据
    输入假设：
    it 迭代器, 其出现顺序是按年份的倒序
    max_years 整形数，至多需要的年数
    min_years 整形数，至少需要的年数

    输出规定：
    [('2018', 12018.0),('2017', 12017.0),...,('2010', 12010.0)]
    此列表最长max_years项，若输入数据不足max_years个年份，则按实际年份；若不足min_years个年份，则直接返回None
    """
    lst = [t for t in it]
    len_lst = len(lst)
    lst = lst[0:max_years] if len_lst > max_years else lst
    return lst if len_lst > min_years else None


def _sort_years_fi(fi_lst: Optional[List[Tuple[str, float]]]) -> Optional[List[Tuple[str, float]]]:
    """ 财务指标数据按年份升序排序
    输入假设：
    fi_lst 列表，例如，[('2018', 0.18)，('2017', 0.17), ..., ('2010', 0.10)]。注意：可能为None

    输出假设：
    [('2010', 0.10)，('2011', 0.11), ..., ('2018', 0.18)]。注意：可能为None
    """
    return list(reversed(fi_lst)) if fi_lst is not None else None


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
        return (yrs_gm_lst[i][1] - yrs_gm_lst[i - 1][1]) / yrs_gm_lst[i - 1][1]

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
    # if yrs_mg_lst is None:
    #     return None
    # d = [mgr > 0 for mgr in [1 + mg for year, mg in yrs_mg_lst]]
    # if not all(d):
    #     i = gmean(d)
    #     i = d
    # return (gmean([1 + mg for year, mg in yrs_mg_lst]) - 1, yrs_mg_lst) \
    #     if (yrs_mg_lst is not None and len(yrs_mg_lst) != 0) else None


_get_last_9_years_fi = partial(_get_last_n_years_fi, max_years=9, min_years=5)


def _get_8_years_gm(years_gm: List[Tuple[str, float]]) -> List[Tuple[str, float]]:
    """ 返回最多8年gross margin数据"""
    return years_gm if (years_gm is None or len(years_gm) <= 8) else years_gm[-8:]


def _calc_years_ms(years_gm):
    """ calculate margin stability"""
    avg, std = 0, 0
    # if years_gm is not None:
    #     avg, std = juxt(np.mean, np.std)([gm for year, gm in years_gm])
    #     return avg / std, years_gm
    # return None


def get_8yrs_mg_and_ms(code: str,
                       getter: Callable[[str], pd.DataFrame] = rdb.get_financial_indicator_by_code) -> Optional[dict]:
    """ 获取上市公司8年（或多年）的毛利增长率的几何平均数，并以dict返回。
    输入假设:
    code:
    列表，例如，(-0.02, [('2011', -0.1523), ('2012', 0.2447), ..., ('2018', 0.1009)])。注意，可能为None
    其中-0.02是增长率的几何平均数

    输出规定：
    {'mg': -0.02, 'years_mgr': [('2011', -0.1523), ('2012', 0.2447), ..., ('2018', 0.1009)],
     'ms': 2.87, 'years_mg': [('2011', 0.421819), ('2012', 0.466875), ..., ('2018', 0.206218)]}
    """
    print(code)
    result = pipe(getter(_convert_to_ts_code(code)),
                  _get_yrs_gm,
                  _get_last_9_years_fi,
                  _sort_years_fi,
                  juxt(compose(_calc_gmean_8yrs_mg, _calc_8yrs_mg),
                       compose(_calc_years_ms, _get_8_years_gm))
                  )
    if result[0] is not None:
        return {'mg': result[0][0], 'years_mgr': result[0][1], 'ms': result[1][0], 'years_mg': result[1][1]}
    else:
        return None


def get_mm(code: str) -> Tuple[float, float]:
    """ maximum margin, MM
    MM = MAX[MS_Percentile(code), MG_Percentile(code)]
    """
    securities = cts.get_securities()[lambda x: (x['industry'] != '银行') & (x['industry'] != '保险')
                                                & (x['industry'] != '证券') & (x['industry'] != '多元金融')]
    securities = securities['symbol'].tolist()
    # securities = ['300025', '300026', '300027']
    mg_ms = [(code, get_8yrs_mg_and_ms(code)) for code in securities]


def _filter_valid_mg_data(gm: pd.DataFrame) -> Iterator[pd.DataFrame]:
    """
    寻找‘合格的’毛利率数据
    所谓合格：1. 排除科创版公司；
             2. 从最近公布的财报开始，必须至少有连续6年的数据

    :param gm: pd.DataFrame
        这个DF包括了多重索引ts_code/end_date和数据栏位grossprofit_margin
        已经排除了异常毛利率的数据，例如，数据为空，<=0%或>100%

    :return: Iterator[pd.DataFrame]
        迭代器中的元素的格式同gm，但仅包含一个公司的数据
    """

    def remain_last_several_years(group):
        """ 仅保留有连续正常毛利率的（年份）数据（从最近年份开始）"""
        last_years = reduce(lambda x, y: y if int(x[:4]) - int(y[:4]) == 1 else x,
                            reversed(group.index.get_level_values('end_date').values))
        filter_mask = [False if end_date < last_years else True
                       for end_date in reversed(group.index.get_level_values('end_date').values)]
        return group[filter_mask]

    return pipe(gm,
                lambda x: x[[True if ts_code[:3] != '688' else False  # 排除科创板上市公司
                             for ts_code in x.index.get_level_values('ts_code').values]],
                lambda x: (group for name, group in x.groupby(level='ts_code')),  # 迭代每一个上市公司
                lambda x: (remain_last_several_years(group) for group in x),  # 保留连续正常的数据
                lambda x: (group for group in x if len(group) >= 6))  # 至少需要六年正常


def _calc_mg_ms(gm_it: Iterator[pd.DataFrame]) -> Iterator[Tuple[str, float, float]]:
    """
    根据毛利率计算盈利能力成长性指标和盈利能力稳定性指标
    MG = (II(1 + ΔGM / GM)) ^ 1/T - 1，盈利能力成长性指标
    MS = Avg(GM) / SD(GM), 盈利能力稳定性指标

    :param gm_it: 迭代器，其中元素是DataFrame，包含ts_code/end_date多重索引和grossprofit_margin栏位

    :return: 迭代器，每个元素中第一项是公司代码，第二项为MG，第三项为MS
    """

    def calc_ms_mg(group: pd.DataFrame) -> Tuple[float, float]:
        """ 计算单个股票的盈利稳定性和几何平均毛利增长率"""
        gm_lst = group['grossprofit_margin'].tolist()
        growth_rate = [(1 + (gm_lst[i + 1] - gm_lst[i]) / gm_lst[i])
                       for i in range(len(gm_lst) - 1)]
        return geometric_mean(growth_rate) - 1, mean(gm_lst) / stdev(gm_lst)

    return ((group.index.get_level_values('ts_code').values[0], *calc_ms_mg(group)) for group in gm_it)


def _calc_ms_mg_quantiles(mg_ms_it: Iterator[Tuple[str, float, float]], n=100) -> [Tuple[List[float], List[float]]]:
    """
    根据输入的全市场盈利能力成长性指标和盈利能力稳定性指标，计算并返回分位表

    :param mg_ms_it: 迭代器，每个元素中第一项是公司代码，第二项为MG，第三项为MS

    :return: 元组，第一项为盈利能力成长性指标的分位表，长度n+1;
                    第二项为盈利能力稳定性指标的分位表，长度n+1;
                    第三项是输入参数的拷贝
    """
    it1, it2, it3 = tee(mg_ms_it, 3)
    return quantiles([value[1] for value in it1], n=n), \
           quantiles([value[2] for value in it2], n=n), \
           it3


def _calc_ms_mg_ranks(data: Tuple[Tuple[List[float], List[float], Iterator[Tuple[str, float, float]]]]) \
        -> List[Tuple[str, float, int, float, int]]:
    """
    计算各个公司的MG和MS百分排位

    :param data: 元组
        每个元素的第一项保存MG的百分分位表；
        每个元素的第二项保存MS的百分分位表
        每个元素的第二项是迭代器，保存各个公司的（代码, mg, ms)
    :return: 列表，每个元素都是列表，其中第一项是公司代码、MG、MG rank、MS、and MS rank
    """
    mg_percentiles = data[0]
    ms_percentiles = data[1]
    assert len(ms_percentiles) == 99 and len(mg_percentiles) == 99

    rtn: List[Tuple] = []
    for code, mg, ms in data[2]:
        mg_rank = 0
        ms_rank = 0
        for i in reversed(range(99)):
            if mg > mg_percentiles[i] and mg_rank == 0:
                mg_rank = i
            if ms > ms_percentiles[i] and ms_rank == 0:
                ms_rank = i
            if ms_rank != 0 and mg_rank != 0:
                break
        rtn.append((code, mg, mg_rank, ms, ms_rank))

    return rtn


def calc_and_save_maximum_margin() -> None:
    """
    计算并保存毛利成长性和稳定性

    :return: None

    Notes:
    This is a impure function.
    --------
    """
    return pipe(rdb.get_financial_indicator(),
                _filter_valid_mg_data,
                _calc_mg_ms,
                _calc_ms_mg_quantiles,
                _calc_ms_mg_ranks,
                rdb.save_profitability_index_to_db)


def get_mg_ms(code: str) -> Optional[Dict]:
    """
    获取某个公司的盈利成长性和盈利稳定性指标（和全市场百分位）
    其中，maximum margin, MM
    MM = MAX(ms_rank, mg_rank)
    :param code: 符合6位数公司代码
    :return: 元组，分别为mm, mg, mg rank, ms, ms rank
    Note: 如果输入值code没能找到对应的盈利能力指标，返回None，代表数据库中数据不足。
    """
    try:
        indicator = rdb.read_profitability_index().loc[_convert_to_ts_code(code)]
        mg_rank = int(indicator.loc['mg_rank'])
        ms_rank = int(indicator.loc['ms_rank'])
        maximum_margin = max(mg_rank, ms_rank)
        return {'code': code,
                'mm': maximum_margin,
                'mg': indicator.loc['mg'],
                'mg_rank': mg_rank,
                'ms': indicator.loc['ms'],
                'ms_rank': ms_rank}
    except KeyError:
        return None


if __name__ == "__main__":
    print(get_mg_ms('600138'))
