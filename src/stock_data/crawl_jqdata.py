import datetime as dt

import sqlalchemy
from jqdatasdk import *

from src import config


if __name__ == "__main__":
    auth(config.jq_user, config.jq_pwd)
    df = get_all_securities(['stock'], dt.datetime.now())
    print(df)
    df.to_sql('securities', con=sqlalchemy.create_engine('sqlite:///../../data/jq.db'), if_exists='replace',
              chunksize=1024)
