import sys, os, logging
import numpy as np
import pandas as pd
import datetime as dt
import partridge as ptg
from itertools import izip

# utils
def get_keys(store_path):
    n = pd.HDFStore(store_path)
    keys = n.keys()
    n.close()
    return keys

# Aggregation functions for pandas
def meantime(series, ignore_date=True, normalize=True, zero_hour=3):
    '''
    in: series is a series of datetime objects
    out: datetime object representing the average of series
    '''
    if ignore_date:
        midnight = series.map(lambda x: dt.datetime(x.year, x.month, x.day))
    series = series - midnight
    if normalize:
        series = normalize_timedelta(series, zero_hour)
    return series.mean()

def stdtime(series, ignore_date=True, normalize=True, zero_hour=3):
    '''
    in: series is a series of datetime objects
    out: datetime object representing the average of series
    '''
    if ignore_date:
        midnight = series.map(lambda x: dt.datetime(x.year, x.month, x.day))
    series = series - midnight
    if normalize:
        series = normalize_timedelta(series, zero_hour)
    return series.std()

def apply_diff(x, val1, val2):
    return x[val1] - x[val2]

def apply_calc_runtime(x):
    return x['last_arrival_time'] - x['first_arrival_time']

def apply_calc_movetime(x): 
    return x['observed_runtime'] - x['observed_stopped_time']
        
def normalize_timedelta(series, zero_hour=0):
    '''
    Normalize a timedelta series around a zero hour
    '''
    z = dt.timedelta(hours=zero_hour)
    d = dt.timedelta(hours=24)
    series.loc[series < z] += d
    
    shifted = pd.Series(series, copy=True)
    shifted.loc[shifted < shifted.mean()] += d
    
    if shifted.std() < series.std():
        return shifted
    else:
        return series

def datetime_to_timedelta(d):
        try:
            return d - dt.datetime(d.year, d.month, d.day)
        except:
            return pd.NaT
        
def datetime_to_seconds(d):
        if not isinstance(d, dt.datetime):
            raise Exception("expected datetime, got %s" % (type(d)))
        return d.hour*3600.0 + d.minute*60.0 + d.second*1.0 + d.microsecond/1000000.0 + d.nanosecond/1000000000.0

def agg_mean(df, value_field='mean', n_field='n'):
    df['wgtval'] = df[value_field] * df[n_field] # TODO: should check that 'wgtval' isn't taken...
    return df['wgtval'].sum() / df[n_field].sum()

def agg_std(df, mean_field, std_field='std', n_field='n'):
    # using formula for variance: https://stats.stackexchange.com/questions/121107/is-there-a-name-or-reference-in-a-published-journal-book-for-the-following-varia
    df.loc[:,'mean'] = df[mean_field].map(lambda x: x.total_seconds())
    df.loc[:,'std'] = df[std_field].map(lambda x: x.total_seconds())
    
    df['wgtval'] = df['mean'] * df[n_field] # TODO: should check that 'wgtval' isn't taken...
    mean = df['wgtval'].sum() / df[n_field].sum()

    df.loc[:,'var'] = np.power(df['std'],2)
    df.loc[:,'val'] = df[n_field] * np.power(df['mean'] - mean,2) + (df[n_field] - 1) * df['var']
    
    try:
        r = np.sqrt(df['val'].sum() / (df[n_field].sum() - 1))
        r = dt.timedelta(seconds=r)
    except:
        r = pd.NaT
    return r
    
def agg_trip_runtime(df, start_field, stop_field, groupby, sortby):
    df = df.sort_values(by=sortby)
    df = df.groupby(groupby).agg({start_field:'first', stop_field:'last'})
    runtime = df[stop_field] - df[start_field]
    return runtime