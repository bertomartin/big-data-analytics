from pandas import Series, DataFrame
import pandas as pd
import numpy as np
import pdb

'''
This file will have a bunch of functions to generate specific features.
'''

def up_days(ohlc_file, feature_dict, target_dict):
    '''(string) -> None

    calculate the number and sum of up-days in the quarter and if the fraction of
    the sum to the total number is > 50%, set the feature to 1, else 0. Add to a feature
    dictionary indexed by year,quarter
    '''
    df = pd.read_csv(ohlc_file)
    df['upday'] = np.where(df['close'] - df['open'] > 0, 1, 0)
    grouped = df.groupby(['fiscal_year', 'fiscal_quarter'])[['upday', 'beat']]

    for k, val in grouped:
        cnt = val['upday'].count()
        sm = val['upday'].sum()
        mx = val['beat'].max()
        feature_dict[k] = [1 if float(sm)/cnt > 0.5 else 0]
        target_dict[k] = mx

def all_features(feature_file, combined_file):
    '''(string, string)->None
    run all the feature function and creates a data frame to hold all their results
    '''
    # gather features
    feature_dict = {}
    target_dict = {} #just get it from the up_days feature function, should refactor
    up_days(combined_file, feature_dict, target_dict) #feature_dict should be updated

    print_str = "yr,qtr,up_day,target\n"
    for item in feature_dict.items():
        key = item[0]
        yr, qtr = key
        target = target_dict[key] #beat for this yr,qtr
        up_day = feature_dict[key][0] #up day is the first feature
        print_str = print_str + "{0},{1},{2},{3}\n".format(yr,qtr,up_day,target)
    fout = open(feature_file, 'wt')
    fout.write(print_str)
    fout.close()

if __name__ == "__main__":
    all_features("./test-stocks/ADBE_features.csv", "./test-stocks/ADBE_combined.csv")
