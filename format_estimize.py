import pandas as pd
import numpy as np
import csv
from dateutil.parser import parse
from glob import glob
import pdb

def remove_lines():
    '''(string) -> None

    Remove the years 2017, 2016, 2015 and 1999 from estimze files
    '''
    estimize_files = glob('./estimize/*')
    for f in estimize_files:
        df = pd.read_csv(f, na_values=['None']).fillna(0)
        mask = ((df['fiscal_year'] == 2018) | (df['fiscal_year'] == 2017) | (df['fiscal_year'] == 2016) | (df['fiscal_year'] == 2015) | (df['fiscal_year'] == 1999))
        rev_mask = np.invert(mask)
        df2 = df[rev_mask] #new data frame with only what we want
        df2.index=df2['date'].apply(parse)
        df3 = calculate_beat(df2)
        df3.to_csv(f)

def calculate_beat(df):
    '''(DataFrame) -> DataFrame

    calculate the difference between eps and wall street estimate. create an additional
    dataframe column. when eps is greater, put 1, when less put 0. return new dataframe
    '''
    df['beat'] = np.where(df['eps'].map(float) - df['wallstreet_estimates'].map(float) > 0, 1, 0)
    return df

if __name__ == "__main__":
    remove_lines()
