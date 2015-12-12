import pandas as pd
import numpy as np
import csv
from dateutil.parser import parse
import pdb
def remove_lines(file_name):
    '''(string) -> None

    Remove the years 2017, 2016, 2015 and 1999 from estimze files
    '''
    df = pd.read_csv(file_name)
    mask = ((df['fiscal_year'] == 2017) | (df['fiscal_year'] == 2016) | (df['fiscal_year'] == 2015) | (df['fiscal_year'] == 1999))
    rev_mask = np.invert(mask)
    df2 = df[rev_mask] #new data frame with only what we want
    df2.index=df2['date'].apply(parse)
    return df2

def write_to_csv(df, file_name):
    '''(DataFrame, file_name) -> None

    writes the dataframe to csv file
    '''
    df.to_csv(file_name)

def calculate_beat(df):
    '''(DataFrame) -> DataFrame

    calculate the difference between eps and wall street estimate. create an additional
    dataframe column. when eps is greater, put 1, when less put 0. return new dataframe
    '''
    df['beat'] = np.where(df['eps'].map(float) - df['wallstreet_estimates'].map(float) > 0, 1, 0)
    return df
    # pdb.set_trace()

if __name__ == "__main__":
    df = remove_lines("./test-stocks/ADBE_estimize.csv")
    df1 = calculate_beat(df)
    write_to_csv(df1, "./test-stocks/ADBE_estimize.csv")
