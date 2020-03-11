from typing import List, Tuple, Iterator, NoReturn
from functools import reduce

from toolz import pipe
import pandas as pd
import numpy as np

from src.stock_data import rim_db as rdb


def _calc_delta_ato() -> List[Tuple[str, float, float]]:
    """
    计算上市公司的经营资产周转率的增加值

    :param today: datetime.date or str

    :return:
    """
    pass


def _calc_and_save_delta_ato() -> NoReturn:
    """
    根据最近报告的资产负债表，计算并保存非金上市公司的年度经营资产周转率、增加值以及增加值的行业排序

    :return: NoReturn
    """
    oa_subjects: List[str] = ['notes_receiv', 'accounts_receiv', 'oth_receiv', 'prepayment', 'inventories', 'amor_exp',
                              'nca_within_1y', 'oth_cur_assets', 'oth_assets', 'lt_rec', 'fix_assets', 'cip',
                              'const_materials', 'fixed_assets_disp', 'produc_bio_assets', 'oil_and_gas_assets',
                              'intan_assets', 'r_and_d', 'goodwill', 'lt_amor_exp', 'defer_tax_assets', 'oth_nca']

    def _iterate_by_code(statements: pd.DataFrame) -> Iterator[pd.DataFrame]:
        return (group.sort_values(by='end_date')[-3:] for idx, group in statements.groupby(level='ts_code'))

    def _calc_amount(security: pd.DataFrame, subjects: List[str]) -> float:
        return [(security.index[0], reduce(lambda x, y: x + float(security[y][0]), subjects, 0)) for i in range(2)]

    def _add_oa(statement):
        statement['oa'] = statement['notes_receiv'] + statement['accounts_receiv'] + statement['oth_receiv'] \
                          + statement['prepayment'] + statement['inventories'] + statement['amor_exp'] \
                          + statement['nca_within_1y'] + statement['oth_cur_assets'] + statement['oth_assets'] \
                          + statement['lt_rec'] + statement['fix_assets'] + statement['cip'] \
                          + statement['const_materials'] + statement['fixed_assets_disp'] \
                          + statement['produc_bio_assets'] + statement['oil_and_gas_assets'] \
                          + statement['intan_assets'] + statement['r_and_d'] + statement['goodwill'] \
                          + statement['lt_amor_exp'] + statement['defer_tax_assets'] + statement['oth_nca'] \
                          + statement['hfs_assets']
        return statement

    def _add_ol(statement):
        statement['ol'] = statement['notes_payable'] + statement['acct_payable'] + statement['adv_receipts'] \
                          + statement['payroll_payable'] + statement['taxes_payable'] + statement['oth_payable'] \
                          + statement['acc_exp'] + statement['deferred_inc'] + statement['oth_cur_liab'] \
                          + statement['lt_payable'] + statement['specific_payables'] + statement['estimated_liab'] \
                          + statement['defer_tax_liab'] + statement['defer_inc_non_cur_liab'] + statement['oth_ncl'] \
                          + statement['lt_payroll_payable'] + statement['hfs_sales']
        return statement

    def _add_noa(statement: pd.DataFrame) -> pd.DataFrame:
        statement['noa'] = statement['oa'] - statement['ol']

    def _transfer_str_to_float(statements: pd.DataFrame) -> pd.DataFrame:
        statements[['produc_bio_assets', 'oil_and_gas_assets', 'acc_exp', 'deferred_inc', 'hfs_sales']] = \
            statements[['produc_bio_assets', 'oil_and_gas_assets', 'acc_exp', 'deferred_inc', 'hfs_sales']] \
                .astype(float)
        return statements

    def _add_ato(statement: pd.DataFrame) -> pd.DataFrame:
        pass

    test = pipe(rdb.get_ts_statement('balancesheet'),
                lambda x: x[x['comp_type'] == '1'],
                lambda x: x.fillna(0),
                _transfer_str_to_float,
                _add_oa,
                _add_ol,
                _add_noa)
    # _iterate_by_code,
    # lambda x: (_calc_amount(security, oa_subjects) for security in x))
    t = [d for d in test]
    a = 2


if __name__ == "__main__":
    _calc_and_save_delta_ato()
