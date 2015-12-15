import pandas as pd
import numpy as np
import csv
from dateutil.parser import parse
from glob import glob
import pdb

def format_ohlc():
    '''(string) -> None

    removes the time column and indexes the dataframe by date. overwrites stock file
    with this new information
    '''
    stock_files = glob('./stocks/*')
    for f in stock_files:
        df = pd.read_csv(f)
        df.drop('time', axis=1, inplace=True)
        df.index=df['date'].apply(parse)
        df.drop('date', axis=1, inplace=True)
        df.to_csv(f)

if __name__ == "__main__":
    format_ohlc()
