from functools import lru_cache
from typing import Callable, NamedTuple
from collections import namedtuple

import pandas as pd
import numpy as np

import rim_db


RimProposal = namedtuple('RimProposal', ['code', 'bps_2018', 'eps_2018',
                                         'eps_2019', 'eps_2020', 'eps_2021'])


def _is_A_list_company_symbol(code: str) -> bool:
    """测试code是否是符合A股代码要求的字符串
    Precondition
    =======================================================================================
    :param code: A股代码

    Post condition
    =======================================================================================
    :return: True or False

    Examples:
    =======================================================================================
    >>> _is_A_list_company_symbol('300072')
    True

    >>> _is_A_list_company_symbol(300072)
    Traceback (most recent call last):
    ...
    AssertionError

    >>> _is_A_list_company_symbol('300072.SZ')
    False

    >>> _is_A_list_company_symbol('700072.SZ')
    False
    """
    assert isinstance(code, str)
    return code.isdigit() \
           and len(code) == 6 \
           and code[:3] in ('000', '002', '300', '600', '603', '608')


@lru_cache(maxsize=4096)
def build_rim_proposal(code: str,
                       get_indicator: Callable[[str], pd.DataFrame] = rim_db.get_indicator,
                       get_eps_forecast: Callable[[str], pd.DataFrame] = rim_db.get_profit_forecast) -> NamedTuple:
    """ 构建用于计算RIM的建议数据

    Precondition
    ===================================================================================
    :param code: 符合A股上市公司代码的要求
    :param get_indicator: 回调函数，返回DataFrame，index是上市公司代码，包含bps,eps列
    :param get_eps_forecast: 回调函数，返回DataFrame，index是上市公司代码，包含eps_2019, eps_2020, eps_2021列

    Post condition
    ===================================================================================
    :return: 若某个股票没有eps预测值，则返回零
    """
    assert _is_A_list_company_symbol(code)

    is_nan = lambda x: 0 if np.isnan(x) else x

    code_indicator = get_indicator('2018').loc[code]
    code_eps_forecast = get_eps_forecast('2020-03-12').loc[code]
    return RimProposal(code=code, bps_2018=code_indicator['bps'], eps_2018=code_indicator['eps'],
                       eps_2019=is_nan(code_eps_forecast['eps_2019']),
                       eps_2020=is_nan(code_eps_forecast['eps_2020']),
                       eps_2021=is_nan(code_eps_forecast['eps_2021']))


if __name__ == "__main__":
    import doctest
    # doctest.testmod()
    print(build_rim_proposal('000625'))
