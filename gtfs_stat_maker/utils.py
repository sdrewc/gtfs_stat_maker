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

def datetime_to_seconds(d):
        if not isinstance(d, dt.datetime):
            raise Exception("expected datetime, got %s" % (type(d)))
        return d.hour*3600.0 + d.minute*60.0 + d.second*1.0 + d.microsecond/1000000.0 + d.nanosecond/1000000000.0

def agg_mean(df, value_field='mean', n_field='n'):
    df['wgtval'] = df[value_field] * df[n_field] # TODO: should check that 'wgtval' isn't taken...
    return df['wgtval'].sum() / df[n_field].sum()

def agg_std(df, mean_field, std_field='std', n_field='n'):
    # using formula for variance: https://stats.stackexchange.com/questions/121107/is-there-a-name-or-reference-in-a-published-journal-book-for-the-following-varia
    #df = pd.DataFrame(df, copy=True)
    df.loc[:,'mean'] = df[mean_field].map(lambda x: x.total_seconds())
    df.loc[:,'std'] = df[std_field].map(lambda x: x.total_seconds())
    
    df['wgtval'] = df['mean'] * df[n_field] # TODO: should check that 'wgtval' isn't taken...
    mean = df['wgtval'].sum() / df[n_field].sum()

    #mean = agg_mean(df, mean_field, n_field).total_seconds() # TODO: should check that 'wgtmean', 'var', 'val' are not taken
    df.loc[:,'var'] = np.power(df['std'],2)
    df.loc[:,'val'] = df[n_field] * np.power(df['mean'] - mean,2) + (df[n_field] - 1) * df['var']

    #print df.dtypes
    #print len(df)
    
    try:
        r = np.sqrt(df['val'].sum() / (df[n_field].sum() - 1))
        r = dt.timedelta(seconds=r)
        #print r
        #self.log.debug(dt.timedelta(seconds=r))
    except:
        r = pd.NaT
    return r

#def agg_std(df, mean_field, std_field='std', n_field='n'):
#    # using formula for variance: https://stats.stackexchange.com/questions/121107/is-there-a-name-or-reference-in-a-published-journal-book-for-the-following-varia
#    mean = agg_mean(df, mean_field, n_field) # TODO: should check that 'wgtmean', 'var', 'val' are not taken
#    if isinstance(mean, np.timedelta64):
#        mean = mean.item() / 10**9 
#    # conversions. convert to seconds in float if datetimes are passed
#    conversions = []
##    df[mean_field] = df[mean_field].map(lambda x: x.total_seconds()).fillna(0)
##    df[std_field] = df[std_field].map(lambda x: x.total_seconds()).fillna(0)
#    for f in [mean_field, std_field]:
#        if pd.core.common.is_timedelta64_dtype(df[f]) or pd.core.common.is_datetime64_dtype(df[f]):
#            df[f] = df[f].map(lambda x: x.total_seconds()).fillna(0)
#            conversions.append(f)
#
#    df.loc[:,'var'] = np.power(df[std_field],2)
#    df.loc[:,'val'] = df[n_field] * np.power(df[mean_field] - mean,2) + (df[n_field] - 1) * df['var']
#    
#    try:
#        r = np.sqrt(df['val'].sum() / (df[n_field].sum() - 1))
#        if len(conversions) > 0:
#            r = dt.timedelta(seconds=r)
#    except:
#        r = np.nan
#    
#    # convert back
#    #for f in conversions:
#    #    df.loc[:,f] = df[f].map(lambda x: dt.timedelta(seconds=x))
#        
#    return r

def agg_mean_and_std(self, df, groupby, mean_field, std_field, n_field):
    # conversions. assumes datetime and timedelta objects for mean, std, repectively
    df['mean'] = df[mean_field].map(lambda x: x.total_seconds())
    df['std'] = df[std_field].map(lambda x: x.total_seconds())
    
    # aggregations
    grouped = df.groupby(groupby)
    mean = grouped.apply(agg_mean, value_field='mean', n_field=n_field)
    std  = grouped.apply(agg_std,  mean_field='mean', std_field='std', n_field=n_field)
    n    = grouped[n_field].sum()
    agg = pd.DataFrame(index=mean.index)
    agg.loc[:,'mean'] = mean
    agg.loc[:,'std'] = std
    agg.loc[:,n_field] = n