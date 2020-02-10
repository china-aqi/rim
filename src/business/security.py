from typing import List, Tuple, Callable

import pandas as pd

from src.stock_data import read_db as rdb


def get_securities(getter: Callable[[], pd.DataFrame]) -> List[Tuple[str, str, str]]:
    """ 获取股票列表，每个项目的内容包括股票名称、代码和拼音简称
    """
    return [(t.index[:6], t.display_name, t.name) for t in getter().itertuples(index=False)]


if __name__ == "__main__":
    print(get_securities(rdb.get_securities))




