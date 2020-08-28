import os

import pandas as pd
from sqlalchemy import create_engine
import numpy as np


def create_empty_db(db_name):
    engine = create_engine('sqlite:///' + db_name + '.db', echo=False)
    return engine


def rename_entsoe_cols(path, df):
    '''
    Renames the dataframe columns names (from the original entso-e's name)
    depending on the nature of the data (prices or load)
    :param path: str, path to the csv file
    :param df: unmodified dataframe of the csv file
    :return:
    '''
    if "Prices" in path:
        df = df.rename(columns={df.columns[1]: "DAMprice"})
    elif "Load" in path:
        df = df.rename(columns={df.columns[1]: "loadForecast", df.columns[2]: "load"})
        df = df.drop(columns=['load'])
    else:
        print("The given data is neither load neither prices")
        return
    return df


def csv_to_df(path):
    '''
    Transforms entso-e's load or prices csv file to df
    :param path: str, path to the csv file
    :return df: dataframe with the csv file data in columns and a datetime index
    '''
    df = pd.read_csv(path)
    df = rename_entsoe_cols(path, df)
    df = df.rename(columns={df.columns[0]: 'mtu'})
    df.index = pd.to_datetime([d[:16] for d in df.mtu.values], format='%d.%m.%Y %H:%M')
    df = df.drop(columns=['mtu'])
    df.replace('-', np.nan, inplace=True)  # Remove empty values
    df.dropna(inplace=True)
    df = df.astype(np.float64)
    # df = df.tz_localize("CET", ambiguous='infer')
    return df


def csv_to_sql(paths_list, sql_name, engine, replace=True):
    '''
    Creates a table in a SQL database containing the data of a the csv files indicated in a list
    :param paths_list: list, elements are the string paths of each file
    :param sql_name: str, name of the sql table
    :param engine: Engine, sql database engine
    :param replace: bool, if True the data replaces the existing table, otherwise is appended
    :return df: Concatenate dataframe of the csv files data
    '''
    df = pd.concat([csv_to_df(p) for p in paths_list])
    df.to_sql(sql_name, con=engine, if_exists="replace" if replace else "append")
    return df


if __name__ == '__main__':
    ################## CREATE A DATABASE WITH THE YEARLY FILES OF PRICES AND LOAD
    PATH_prices = 'data/prices'
    PATH_load = 'data/load'

    table_load = 'load'
    table_prices = 'DAMprice'

    paths_prices = ["%s/%s" % (PATH_prices, file) for file in os.listdir(PATH_prices)]
    paths_load = ["%s/%s" % (PATH_load, file) for file in os.listdir(PATH_load)]

    engine = create_empty_db('db')
    df_p_0 = csv_to_sql(paths_prices, table_prices, engine)
    df_l_0 = csv_to_sql(paths_load, table_load, engine)

    ################## UPDATE THE DATABASE WITH THE NEW PRICES AND LOAD
    PATH_update = 'data/buffer'
    paths_new = ["%s/%s" % (PATH_update, file) for file in os.listdir(PATH_update)]
    try:
        df_l_1 = csv_to_sql([x for x in paths_new if "Load" in x], table_load, engine, replace=False)
        df_l_raw = pd.concat([df_l_0, df_l_1])
    except ValueError:
        df_l_raw = df_l_0

    try:
        df_p_1 = csv_to_sql([x for x in paths_new if "Prices" in x], table_prices, engine, replace=False)
        df_p_raw = pd.concat([df_p_0, df_p_1])
    except ValueError:
        df_p_raw = df_p_0

    # # Clip data
    df_p = df_p_raw.clip(lower=df_p_raw.quantile(0.01).values.item(), upper=df_p_raw.quantile(0.99).values.item(),
                         inplace=False)
    df_l = df_l_raw.clip(lower=df_l_raw.quantile(0.01).values.item(), upper=df_l_raw.quantile(0.99).values.item(),
                         inplace=False)
    df_p.plot()
    df_p.hist(bins=100)
    df_l.plot()
    df_l.hist(bins=100)

    # Remake index and check for nan
    new_index = pd.date_range(start='1/1/2017 00:00:00', end='26/8/2020 23:00:00', freq='H', tz='CET')
    df_p = df_p.tz_localize("CET", ambiguous='infer')
    # print(df_p.index[df_p.index.duplicated()])
    df_p.reindex(new_index)
    df_p.isnull().values.any()

    df_l = df_l.tz_localize("CET", ambiguous='infer')
    df_l_h = df_l.resample("1h").apply(np.mean)
    df_l_h.reindex(new_index)
    df_l_h.isnull().values.any()