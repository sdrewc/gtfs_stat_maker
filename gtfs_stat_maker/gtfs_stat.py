__author__      = "Drew Cooper"

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
    mean = agg_mean(df, mean_field, n_field) # TODO: should check that 'wgtmean', 'var', 'val' are not taken
    df['var'] = np.power(df[std_field],2)
    df['val'] = df[n_field] * np.power(df[mean_field] - mean,2) + (df[n_field] - 1) * df['var']
    
    try:
        r = np.sqrt(df['val'].sum() / (df[n_field].sum() - 1))
    except:
        return np.nan
    return r

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
    
class gtfs_to_apc():
    def __init__(self, gtfs_route_cols=None, gtfs_trip_cols=None, gtfs_stop_time_cols=None, 
                 apc_route_cols=None, apc_trip_cols=None, apc_stop_time_cols=None):
        '''
        User may provide columns that uniquely identify a gtfs route, trip, and stop, and
        apc route, trip, and stop.  If no column names are provided, defaults will be used.
        Defaults:
            gtfs_route_cols = ['route_id']
            gtfs_trip_cols = ['trip_id']
            gtfs_stop_cols = ['stop_id']
            
        '''
        self.trip_cols = ['gtfs.trip_id', 'apc.trip_id']
        self.route_cols = []
        self.stop_cols = []
        
        self.trips = None
        self.stops = None
        self.routes = None
        
class stats():
    def __init__(self, apc_hdf, gtfs_paths, distributed=False, config_file=None, nodes=None, logger=None, depends=None):
        self.apc_path = apc_hdf
        self.apc_keys = get_keys(self.apc_path)
        self.date_ranges = None
        self.dow_by_service_id = None
        self.dow_count_by_service_id = None
        self.distributed = distributed
        self.config_file = config_file
        self.log = logger
        # APC Aggregations
        self._default_groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL']
        self._default_stat_args = {'ARRIVAL_TIME':[meantime,stdtime,'size']}
        self._default_reagg_args = {'meantime':{agg_mean:{'value_field':'meantime',
                                                          'n_field':'size'}},
                                    'stdtime': {agg_std: {'mean_field':'meantime',
                                                          'std_field':'stdtime',
                                                          'n_field':'size'}},
                                    'size':    'size'}
        self._apc_stop_time_stats = None
        
        # GTFS-STAT
        self.gtfs_to_apc     = None 
        self.route_stats     = None
        self.trip_stats      = None
        self.stop_time_stats = None
        
        if isinstance(gtfs_paths, str):
            gtfs_paths = [gtfs_paths]
            
        sids = []
        self.log.info('getting service_id info from gtfs paths')
        for idx, gtfs_path in izip(range(len(gtfs_paths)), gtfs_paths):
            service_ids_by_date = ptg.read_service_ids_by_date(gtfs_path)
            service_ids_by_date = pd.DataFrame.from_dict(service_ids_by_date, orient='index').reset_index()
            service_ids_by_date.rename(columns={'index':'date', 0:'service_id'}, inplace=True)
            service_ids_by_date['file_idx'] = idx
            sids.append(service_ids_by_date)
        sids = pd.concat(sids)
        sids.loc[:,'weekday'] = sids['date'].map(lambda x: x.weekday())
        
        self.log.info('calculating date ranges and service_id stats')
        self.date_ranges = sids.groupby('file_idx').agg({'date':['min','max']})
        self.date_ranges.columns = ['start_date','end_date']
        self.dow_count_by_service_id = sids.pivot_table(index=['file_idx','service_id'],
                                                        columns=['weekday'], aggfunc='count')
        self.dow_by_service_id = pd.notnull(self.dow_count_by_service_id) * 1
        if self.distributed:
            self.log.info('setting up distributed processing')
            self._setup_distributed_processing(depends, nodes)
        
    def _setup_distributed_processing(self, depends=None, nodes=None):
        self.log.debug('imports for distributed processing')
        global jobs_cond, lower_bound, upper_bound, submit_queue, dispy, pickle, threading
        global job_callback, load_pickle, dump_pickle, config, print_dispy_job_error
        global proc_stop_time_stats, proc_combine_stop_time_stats
        import dispy, threading
        import cPickle as pickle
        from dispy_processing_utils import job_callback, load_pickle, dump_pickle, config, print_dispy_job_error
        from dispy_processing_utils import proc_stop_time_stats, proc_combine_stop_time_stats, filename_generator

        self.log.debug('reading config for distributed processing')
        self.config = config(self.config_file, nodes)
        self.log.debug('setting up job cluster')
        self._default_depends = [meantime, stdtime, load_pickle, dump_pickle, __file__]
        self.depends = self._default_depends if depends==None else depends
        self.cluster = dispy.JobCluster(proc_stop_time_stats, 
                                        callback=job_callback, 
                                        depends=self.depends,
                                        nodes=self.config.nodes, 
                                        loglevel=logging.info)
        self.log.debug('setting up filename generator')
        self.fg = filename_generator(r'C:\Temp\tmp_gtfs_stat')
        self.log.debug('setting up globals')
        jobs_cond = threading.Condition()
        lower_bound = self.config.lower_bound
        upper_bound = self.config.upper_bound
        submit_queue = {}
        self.log.debug('done setting up')
    
    def apc_stop_time_stats(self, groupby=None, stat_args=None):
        '''
        Reads apc data from an h5 file and computes apc stop-time statistics.
        '''
        # TODO move shared code from _distributed and _sequential into here
        if self.distributed:
            self._apc_stop_time_stats_distributed(groupby, stat_args)
        else:
            self._apc_stop_time_stats_sequential(groupby, stat_args)
        return self._apc_stop_time_stats
        
    def reagg_apc_stop_time_stats(self, groupby=None, **kwargs):
        '''
        Assumes that apc data has already been read, and apc_stop_time_stats created.
        Reaggregates self._apc_stop_time_stats to some higher level of aggregation.
        groupby is a set of columns for aggregation
        kwargs is a dict of column-name: aggregation function or dict
            if dict, then it should be aggfunc: kwargs where kwargs are arguments
            required by aggfunc
        '''
        # TODO make a distributed version of this
        columns = [] # list of tuples to make an index or multiindex
        agg_dfs = [] # list of all the aggregations.  Each will be a series with an
                     # index defined by groupby
        groupby = self._default_groupby if groupby==None else groupby
        kwargs = self._default_reagg_args if kwargs==None else kwargs
        
        grouped = self._apc_stop_time_stats.groupby(groupby)
        for column, arg in kwargs.iteritems():
            if isinstance(arg, dict):
                for aggfunc, kas in arg.iteritems():
                    columns.append((column,aggfunc.__name__))
                    agg_dfs.append(grouped.agg({column:aggfunc}, **kas))            
            if isinstance(arg, list):
                for aggfunc in arg:
                    columns.append((column,aggfunc.__name__))
                    agg_dfs.append(grouped.agg({column:aggfunc}))
            else:
                columns.append((column))
                agg_dfs.append(grouped.agg({column:arg}))
        mi = pd.MultiIndex.from_tuples(columns)
        df = pd.DataFrame(agg_dfs[0].index, columns=mi)
        
        for col, agg in izip(columns, agg_dfs):
            df.loc[:,col] = agg
        self._apc_stop_time_stats = df
        return self._apc_stop_time_stats
    
    def _apc_stop_time_stats_sequential(self, groupby=None, stat_args=None):
        # apc data is stored by month (or possibly other chunks)
        groupby = self._default_groupby if groupby==None else groupby
        stat_args= self._default_stat_args if stat_args==None else stat_args
        chunks = []
        for key in self.apc_keys:
            self.log.debug('reading file %s, key %s' % (self.apc_path, key))
            apc = pd.read_hdf(self.apc_path, key)
            
            self.log.debug('updating file_idx')
            for idx, row in self.date_ranges.iterrows():
                apc.loc[apc['DATE'].between(row['start_date'],row['end_date']),'file_idx'] = idx

            # create new attributes
            apc.loc[:,'weekday'] = apc['DATE'].map(lambda x: x.weekday())
            self.log.debug('calculating stop_time_stats')
            stop_time_stats = apc.groupby(groupby).agg(stat_args)
            chunks.append(stop_time_stats)
            
        df = pd.concat(chunks)
        df.columns = df.columns.droplevel()
        df.reset_index(inplace=True)
        self._apc_stop_time_stats = df
        return self._apc_stop_time_stats
        
    def _apc_stop_time_stats_distributed(self, service_id=1, groupby=None, stat_args=None):
        # defaults
        groupby = self._default_groupby if groupby==None else groupby
        stat_args= self._default_stat_args if stat_args==None else stat_args
        chunks = []
        i = 0
        
        # iterate through keys.  apc data is stored by month (or possibly other chunks)
        for key in self.apc_keys:
            self.log.debug('reading file %s, key %s' % (self.apc_path, key))
            apc = pd.read_hdf(self.apc_path, key)
            
            # assign each record to the gtfs feed with corresponding date range
            self.log.debug('updating file_idx')
            for idx, row in self.date_ranges.iterrows():
                apc.loc[apc['DATE'].between(row['start_date'],row['end_date']),'file_idx'] = idx

            # create new attributes
            apc.loc[:,'weekday'] = apc['DATE'].map(lambda x: x.weekday())
            
            wait_queue = {}
            to_merge = []
            ifile, ofile = self.fg.next(), self.fg.next()
            dump_pickle(ifile, apc)
            job = self.cluster.submit(ifile, ofile, groupby, stat_args)
            print "submitting job %d (%s)" % (i, str(idx))
            jobs_cond.acquire()
            job.id = i
            if job.status == dispy.DispyJob.Created or job.status == dispy.DispyJob.Running:
                submit_queue[i] = job
                # wait for queue to fall before lower bound before submitting another job
                if len(submit_queue) >= upper_bound:
                    while len(submit_queue) > lower_bound:
                        jobs_cond.wait()
            jobs_cond.release()
            
            wait_queue[i] = job
            pop_ids = []
            for jobid, job in wait_queue.iteritems():
                if job.status == dispy.DispyJob.Finished:
                    print "finished job %d" % jobid
                    tempfile = job.result
                    pop_ids.append(jobid)
                    to_merge.append(tempfile)
                elif job.status in [dispy.DispyJob.Abandoned, dispy.DispyJob.Cancelled,
                                    dispy.DispyJob.Terminated]:
                    print_dispy_job_error(job)
            for jobid in pop_ids:
                wait_queue.pop(jobid)

            i += 1
        pop_ids = []
        for jobid, job in wait_queue.iteritems():
            try:
                tempfile = job()
                print "finished job %d" % jobid
                print 'TEMPFILE', tempfile
                pop_ids.append(jobid)
                to_merge.append(tempfile)
                print_dispy_job_error(job)
            except Exception as e:
                print e
                print_dispy_job_error(job)
        for jobid in pop_ids:
            wait_queue.pop(jobid)
        print "done waiting"
        for fname in to_merge:
            chunks.append(load_pickle(fname))
        
        df = pd.concat(chunks)
        df.columns = df.columns.droplevel()
        df.reset_index(inplace=True)
        self._apc_stop_time_stats = df
        return self._apc_stop_time_stats
        