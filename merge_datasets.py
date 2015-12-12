from pandas import Series, DataFrame
import pandas as pd
from dateutil.parser import parse
from datetime import timedelta, date
import datetime as dt
import pdb

def data_merge(ohlc_file, estimize_file):
    '''(string, string) -> DataFrame

    merge the estimize dataset into the ohlc dataset and line up the indices. in the merge
    fill forward data from the estimize dataset into the ohlc dataset
    '''
    #load stock data in df
    stock_df = pd.read_csv(ohlc_file)
    stock_df.index=stock_df['date'].apply(parse)
    #load estimize data
    estimize_df = pd.read_csv(estimize_file)
    estimize_df.index=estimize_df['date'].apply(parse)
    #subset estimize data so we only get quarters and beat data
    estimize_df2 = estimize_df.ix[:, ['fiscal_year', 'fiscal_quarter', 'beat']]
    new_estimize = estimize_df2.reindex(stock_df.index, method='ffill')
    # result = pd.concat([df1, df4], axis=1)
    result = pd.concat([stock_df, new_estimize], axis=1).dropna()
    # pdb.set_trace()
    result['fiscal_year'] = result['fiscal_year'].apply(int)
    result['fiscal_quarter'] = result['fiscal_quarter'].apply(int)
    result['beat'] = result['beat'].apply(int)
    result.to_csv("./test-stocks/ADBE_combined.csv")

    # pdb.set_trace()



if __name__ == '__main__':
    data_merge("./test-stocks/ADBE.csv", "./test-stocks/ADBE_estimize.csv")
