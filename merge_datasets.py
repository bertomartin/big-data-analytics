from pandas import Series, DataFrame
import pandas as pd
from dateutil.parser import parse
from datetime import timedelta, date
import datetime as dt
from glob import glob
import pdb

def data_merge():
    '''(string, string) -> DataFrame

    merge the estimize dataset into the ohlc dataset and line up the indices. in the merge
    fill forward data from the estimize dataset into the ohlc dataset
    '''
    #load stock data in df
    stock_files = glob('./stocks/*')
    estimize_files = glob('./estimize/*')
    stock_estimize = zip(stock_files, estimize_files)
    for zipped in stock_estimize:
        stock_f, estimize_f = zipped
        stock_df = pd.read_csv(stock_f)
        stock_df.index=stock_df['date'].apply(parse)

        estimize_df = pd.read_csv(estimize_f)
        estimize_df.index=estimize_df['date'].apply(parse)
        estimize_df2 = estimize_df.ix[:, ['fiscal_year', 'fiscal_quarter', 'beat']]

        estimize_df2 = estimize_df2.sort_index()
        new_estimize = estimize_df2.reindex(stock_df.index, method='bfill')
        # new_estimize = new_estimize.bfill()

        result = pd.concat([stock_df, new_estimize], axis=1).dropna()
        result['fiscal_year'] = result['fiscal_year'].apply(int)
        result['fiscal_quarter'] = result['fiscal_quarter'].apply(int)
        result['beat'] = result['beat'].apply(int)

        name = stock_f.split("/")[2].split(".")[0]
        save_as = "./stocks/" + name + "_combined.csv"
        result.to_csv(save_as)

if __name__ == '__main__':
    data_merge()
