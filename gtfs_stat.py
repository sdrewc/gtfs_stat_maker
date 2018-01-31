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
def meantime(series, ignore_date=True):
    '''
    in: series is a series of datetime objects
    out: datetime object representing the average of series
    '''
    ref = series.iloc[0]
    if ignore_date:
        series = series.map(lambda x: dt.datetime(ref.year,ref.month,ref.day,x.hour,x.minute,x.second))
    series = series - ref # now it's a series of timedelta objects
    return ref + series.mean()

def stdtime(series, ignore_date=True):
    '''
    in: series is a series of datetime objects
    out: datetime object representing the average of series
    '''
    ref = series.iloc[0]
    if ignore_date:
        series = series.map(lambda x: dt.datetime(ref.year,ref.month,ref.day,x.hour,x.minute,x.second))
    series = series - ref # now it's a series of timedelta objects
    return series.std()

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
    
class stats():
    def __init__(self, apc_hdf, gtfs_paths, distributed=False, config_file=None, logger=None):
        self.apc_path = apc_hdf
        self.apc_keys = get_keys(self.apc_path)
        self.date_ranges = None
        self.dow_by_service_id = None
        self.dow_count_by_service_id = None
        self.distributed = distributed
        self.config_file = config_file
        self.log = logger
        
        # APC Aggregations
        self.apc_stop_time_stats = None
        
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
            self._setup_distributed_processing()
        
    def _setup_distributed_processing(self):
        self.log.debug('imports for distributed processing')
        global jobs_cond, lower_bound, upper_bound, submit_queue, dispy, pickle, threading
        global job_callback, load_pickle, dump_pickle, config, print_dispy_job_error
        global proc_stop_time_stats, proc_combine_stop_time_stats
        import dispy, threading
        import cPickle as pickle
        from dispy_processing_utils import job_callback, load_pickle, dump_pickle, config, print_dispy_job_error
        from dispy_processing_utils import proc_stop_time_stats, proc_combine_stop_time_stats, filename_generator

        self.log.debug('reading config for distributed processing')
        self.config = config(self.config_file)
        self.log.debug('setting up job cluster')
        self.cluster = dispy.JobCluster(proc_stop_time_stats, 
                                        callback=job_callback, 
                                        depends=[meantime, 
                                                 stdtime, 
                                                 load_pickle, 
                                                 dump_pickle,
                                                 __file__,
                                                 ],
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
        
    def stop_time_stats():
        pass
    
    def stop_time_stats(self, service_id=1, groupby=None, stat_args=None):
        if self.distributed:
            self._stop_time_stats_distributed(service_id, groupby, stat_args)
        else:
            self._stop_time_stats_sequential(service_id, groupby, stat_args)
        
    def agg_stop_time_stat_chunks(self, service_id=1, **kwargs):
        '''
        aggfunc should be a dict of fieldname: kwargs
        '''
        pass
    
    def _stop_time_stats_sequential(self, service_id=1, groupby=None, stat_args=None):
        # apc data is stored by month (or possibly other chunks)
        i = 0
        groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL'] if groupby==None else groupby
        stat_args= {'ARRIVAL_TIME':[meantime,stdtime,'size']} if stat_args==None else stat_args
        chunks = []
        for key in self.apc_keys:
            self.log.debug('reading file %s, key %s' % (self.apc_path, key))
            apc = pd.read_hdf(self.apc_path, key, stop=1000000)
            
            self.log.debug('updating file_idx')
            for idx, row in self.date_ranges.iterrows():
                apc.loc[apc['DATE'].between(row['start_date'],row['end_date']),'file_idx'] = idx

            # create new attributes
            apc.loc[:,'weekday'] = apc['DATE'].map(lambda x: x.weekday())
            # get the full possible set of stops associated with a trip
            s = apc.groupby(['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ']).agg({'STOP_AVL':pd.Series.nunique})
            if len(s.loc[s['STOP_AVL'].ne(1)]) == 0:
                self.log.warn('block %s has multiple stop_ids for a single combination of file_idx, ROUTE_SHORT_NAME, DIR, PATTCODE, TRIP, SEQ in %d instances' % (key, len(s.loc[s['STOP_AVL'].ne(1)])))
                # if this isn't true, then the pivot below won't work
            canonical_trip_seq = apc.groupby(['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL'], as_index=False).size().reset_index()
            canonical_trip_seq.rename(columns={0:'samples'}, inplace=True)
            canonical_trip_seq.sort_values(by=['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ'], inplace=True)
            self.log.debug('calculating stop_time_stats')
            stop_time_stats = apc.groupby(groupby).agg(stat_args)
            chunks.append(stop_time_stats)
            # TODO -- Fill in the rest from the Jupyter notebook
        
    def _stop_time_stats_distributed(self, service_id=1, groupby=None, stat_args=None):
        # defaults
        groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL'] if groupby==None else groupby
        stat_args= {'ARRIVAL_TIME':[meantime,stdtime,'size']} if stat_args==None else stat_args
        chunks = []
        i = 0
        
        # iterate through keys.  apc data is stored by month (or possibly other chunks)
        for key in self.apc_keys:
            self.log.debug('reading file %s, key %s' % (self.apc_path, key))
            apc = pd.read_hdf(self.apc_path, key, stop=1000000)
            
            # assign each record to the gtfs feed with corresponding date range
            self.log.debug('updating file_idx')
            for idx, row in self.date_ranges.iterrows():
                apc.loc[apc['DATE'].between(row['start_date'],row['end_date']),'file_idx'] = idx

            # create new attributes
            apc.loc[:,'weekday'] = apc['DATE'].map(lambda x: x.weekday())
            
            # get the full possible set of stops associated with a trip
            apc.drop_duplicates(subset=['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL','ARRIVAL_TIME'], inplace=True)
            s = apc.groupby(['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ']).agg({'STOP_AVL':pd.Series.nunique})
            if len(s.loc[s['STOP_AVL'].ne(1)]) == 0:
                self.log.warn('block %s has multiple stop_ids for a single combination of file_idx, ROUTE_SHORT_NAME, DIR, PATTCODE, TRIP, SEQ in %d instances' % (key, len(s.loc[s['STOP_AVL'].ne(1)])))
            canonical_trip_seq = apc.groupby(['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL'], as_index=False).size().reset_index()
            canonical_trip_seq.rename(columns={0:'samples'}, inplace=True)
            canonical_trip_seq.sort_values(by=['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ'], inplace=True)
            
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
        self._stats_by_chunk
        return
        
    def agg_mean_and_std(self, df, groupby, mean_field, std_field, n_field):
        # conversions. assumes datetime and timedelta objects for mean, std, repectively
        df['mean'] = df[mean_field].map(lambda x: datetime_to_seconds(x))
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
        
    