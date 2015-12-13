from pandas import Series, DataFrame
import pandas as pd
import numpy as np
from ta import *
import pdb

'''
This file will have a bunch of functions to generate specific features.
'''

#price rose more than 1/2 the time
def up_days(df, feature_dict, target_dict):
    '''(string) -> None

    calculate the number and sum of up-days in the quarter and if the fraction of
    the sum to the total number is > 50%, set the feature to 1, else 0. Add to a feature
    dictionary indexed by year,quarter
    '''
    df['upday'] = np.where(df['close'] - df['open'] > 0, 1, 0)
    grouped = df.groupby(['fiscal_year', 'fiscal_quarter'])[['upday', 'beat']]

    for k, val in grouped:
        cnt = val['upday'].count()
        sm = val['upday'].sum()
        mx = val['beat'].max()
        feature_dict[k] = [1 if float(sm)/cnt > 0.5 else 0]
        target_dict[k] = mx

# price above 20dma more than 1/2 the time
def price_above_20(df, feature_dict):
    ma20 = MA(df,20)
    ma20 = ma20.dropna()
    ma20['price_above'] = np.where(ma20['close'] - ma20['MA_20'] > 0, 1, 0)
    grouped = ma20.groupby(['fiscal_year', 'fiscal_quarter'])[['MA_20']]
    for k, val in grouped:
        cnt = val['MA_20'].count()
        sm = val['MA_20'].sum()
        feature_dict[k].append(1 if float(sm)/cnt > 0.5 else 0)
    # pdb.set_trace()

def num_vol_up_days(df, feature_dict):
    pass

def num_obv_up_days(df, feature_dict):
    pass

def all_features(feature_file, combined_file):
    '''(string, string)->None
    run all the feature function and creates a data frame to hold all their results
    '''
    # gather features and target
    feature_dict = {}
    target_dict = {} #just get it from the up_days feature function, should refactor
    df = pd.read_csv(combined_file)

    #features
    up_days(df, feature_dict, target_dict) #feature_dict should be updated
    price_above_20(df, feature_dict)

    print_str = "yr,qtr,up_day,p_over_20,target\n"
    for item in feature_dict.items():
        key = item[0]
        yr, qtr = key
        target = target_dict[key] #beat for this yr,qtr
        up_day = feature_dict[key][0] #up day is the first feature
        p_over_20 = feature_dict[key][1]
        print_str = print_str + "{0},{1},{2},{3},{4}\n".format(yr,qtr,up_day,p_over_20,target)
    fout = open(feature_file, 'wt')
    fout.write(print_str)
    fout.close()

if __name__ == "__main__":
    all_features("./test-stocks/ADBE_features.csv", "./test-stocks/ADBE_combined.csv")
