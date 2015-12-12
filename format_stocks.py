import pandas as pd
import numpy as np
import csv
from dateutil.parser import parse
import pdb

def format_ohlc(ohlc_file):
    '''(string) -> None

    removes the time column and indexes the dataframe by date. overwrites stock file
    with this new information
    '''
    df = pd.read_csv(ohlc_file)
    df.drop('time', axis=1, inplace=True)
    df.index=df['date'].apply(parse)
    df.drop('date', axis=1, inplace=True)
    df.to_csv(ohlc_file)


if __name__ == "__main__":
    df = format_ohlc("./test-stocks/ADBE.csv")
