import sqlalchemy
import pandas as pd


def get_securities():
    return pd.read_sql('securities', con=sqlalchemy.create_engine('sqlite:///../../data/jq.db'))


if __name__ == "__main__":
    df = get_securities()
    print(df)
