import os

import pandas as pd
from sqlalchemy import create_engine

def create_empty_db(db_name):
    engine = create_engine('sqlite:///'+db_name+'.db', echo=False)
    return engine

def rename_entsoe_cols(path, df):
    '''
    Renames the dataframe columns names (from the original entso-e's name)
    depending on the nature of the data (prices or load)
    :param path: string path to the csv file
    :param df: unmodified dataframe of the csv file
    :return:
    '''
    if "prices" in path:
        df = df.rename(columns={df.columns[1]: "DAMprice"})
    elif "load" in path:
        df = df.rename(columns={df.columns[1]: "loadForecast", df.columns[2]: "load"})
    else:
        print("The given data is neither load neither prices")
        return
    return df

def csv_to_df(path):
    '''
    Transforms entso-e's load or prices csv file to df
    :param path: string path to the csv file
    :return df: dataframe with the csv file data in columns and a datetime index
    '''
    df = pd.read_csv(path)
    df = rename_entsoe_cols(path, df)
    df = df.rename(columns={df.columns[0]: "mtu"})
    df.index = pd.to_datetime([d[:16] for d in df.mtu.values])
    df = df.drop(columns=["mtu"])
    return df

def csv_to_sql(paths_list, sql_name, engine):
    '''
    Creates a table in a SQL database containing the data of a the csv files indicated in a list
    :param paths_list: list with string paths of each file
    :param sql_name: name of the sql table
    :param engine: sql database engine
    :return df: Concatenate dataframe of the csv files data
    '''
    df = pd.concat([csv_to_df(p) for p in paths_list])
    df.to_sql(sql_name, con=engine, if_exists="replace")
    return df

PATH_prices = 'data/prices'
PATH_load = 'data/load/'

paths_prices = ["%s/%s"%(PATH_prices, file) for file in os.listdir(PATH_prices)]
paths_load = ["%s/%s"%(PATH_load, file) for file in os.listdir(PATH_load)]

engine = create_empty_db('db')
df_p = csv_to_sql(paths_prices, 'DAMprice', engine)
df_l = csv_to_sql(paths_load, 'load', engine)