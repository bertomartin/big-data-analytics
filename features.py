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
        cnt = val['price_above'].count()
        sm = val['price_above'].sum()
        feature_dict[k].append(1 if float(sm)/cnt > 0.5 else 0)
        # pdb.set_trace()

# percentage of times price was above 10d ema above 50?
def price_above_10_ema(df, feature_dict):
    ma10_ema = EMA(df,10)
    ma10_ema = ma10_ema.dropna()
    ma10_ema['price_above'] = np.where(ma10_ema['close'] - ma10_ema['EMA_10'] > 0, 1, 0)
    grouped = ma10_ema.groupby(['fiscal_year', 'fiscal_quarter'])[['EMA_10']]
    for k, val in grouped:
        cnt = val['price_above'].count()
        sm = val['price_above'].sum()
        feature_dict[k].append(1 if float(sm)/cnt > 0.5 else 0)

# percentage of times price went up was above 50?
def price_mom1(df, feature_dict):
    '''
    returns the number of days the price increase
    '''
    mom1 = MOM(df,1)
    mom1 = mom1.dropna()
    mom1['mom'] = np.where(mom1['momentum_1'] > 0, 1, 0)
    grouped = mom1.groupby(['fiscal_year', 'fiscal_quarter'])[['mom']]
    for k, val in grouped:
        cnt = val['mom'].count()
        sm = val['mom'].sum()
        feature_dict[k].append(1 if float(sm)/cnt > 0.5 else 0)

# percentage of times price went up 3times above 50%?
def price_mom3(df, feature_dict):
    pass

# number of times price and volume went up together
def price_and_vol_mom(df, feature_dict):
    pass

# percentage of times volume went up was over 50?
def vol_mom1(df, feature_dict):
    mom1 = MOM_vol(df,1)
    mom1 = mom1.dropna()
    mom1['v_mom'] = np.where(mom1['v_momentum_1'] > 0, 1, 0)
    grouped = mom1.groupby(['fiscal_year', 'fiscal_quarter'])[['v_mom']]
    for k, val in grouped:
        cnt = val['v_mom'].count()
        sm = val['v_mom'].sum()
        feature_dict[k].append(1 if float(sm)/cnt > 0.5 else 0)

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
    price_above_10_ema(df, feature_dict)
    price_mom1(df, feature_dict)
    vol_mom1(df, feature_dict)


    print_str = "yr,qtr,up_day,p_over_20,p_over_10_ema,p_mom_1,v_mom_1,target\n"
    for item in feature_dict.items():
        key = item[0]
        yr, qtr = key
        target = target_dict[key] #beat for this yr,qtr
        up_day = feature_dict[key][0] #up day is the first feature
        p_over_20 = feature_dict[key][1]
        p_above_10_ema = feature_dict[key][2]
        p_mom_1 = feature_dict[key][3]
        v_mom_1 = feature_dict[key][4]
        print_str = print_str + "{0},{1},{2},{3},{4},{5},{6},{7}\n".format(yr,qtr,up_day,p_over_20,p_above_10_ema,p_mom_1,v_mom_1,target)
    fout = open(feature_file, 'wt')
    fout.write(print_str)
    fout.close()

if __name__ == "__main__":
    all_features("./test-stocks/ADBE_features.csv", "./test-stocks/ADBE_combined.csv")
