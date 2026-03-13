import pandas as pd


def inspect_df(df: pd.DataFrame):
    print('\nshape:\n')
    print(df.shape)

    print('\nColumns type:\n')
    print(df.dtypes)

    print('\nCheck if has Null\n')
    print(df.isna().sum())


if __name__ == '__main__':
    ...
