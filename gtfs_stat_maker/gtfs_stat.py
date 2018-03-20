__author__      = "Drew Cooper"

import sys, os, logging
import numpy as np
import pandas as pd
import datetime as dt
import partridge as ptg
from itertools import izip
sys.path.insert(0,os.path.dirname(os.path.realpath(__file__)))
from utils import get_keys, meantime, stdtime, agg_mean, agg_std, normalize_timedelta, datetime_to_seconds, datetime_to_timedelta
import holidays
    
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

class stop_time_stats_settings():
    def __init__(self):
        self.groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL']
        self.stat_args = {'ARRIVAL_TIME':[meantime,stdtime,'size'],
                          'DEPARTURE_TIME':[meantime,stdtime]}
        
        self.rename = {('ARRIVAL_TIME','meantime'):'avg_arrival_time',
                       ('ARRIVAL_TIME','stdtime'):'stdev_arrival_time',
                       ('ARRIVAL_TIME','size'):'samples',
                       ('DEPARTURE_TIME','meantime'):'avg_departure_time',
                       ('DEPARTURE_TIME','stdtime'):'stdev_departure_time'
                       }
        
        self.reagg_args = {'avg_arrival_time':{agg_mean:{'value_field':'avg_arrival_time',
                                                         'n_field':'samples'}},
                           'avg_departure_time':{agg_mean:{'value_field':'avg_departure_time',
                                                           'n_field':'samples'}},
                           'stdev_arrival_time':{agg_std:{'mean_field':'avg_arrival_time',
                                                          'std_field':'stdev_arrival_time',
                                                          'n_field':'samples'}},
                           'stdev_departure_time':{agg_std:{'mean_field':'avg_departure_time',
                                                            'std_field':'stdev_departure_time',
                                                            'n_field':'samples'}},
                           'samples':    np.sum}
                           
class trip_stats_settings():
    def __init__(self):
        self.groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']
        self.order_by = 'SEQ'
        self.stat_args = {'ARRIVAL_TIME':[meantime,stdtime,'size'],
                          'DEPARTURE_TIME':[meantime,stdtime]}
        
        self.rename = {('ARRIVAL_TIME','meantime'):'avg_arrival_time',
                                ('ARRIVAL_TIME','stdtime'):'stdev_arrival_time',
                                ('ARRIVAL_TIME','size'):'samples',
                                ('DEPARTURE_TIME','meantime'):'avg_departure_time',
                                ('DEPARTURE_TIME','stdtime'):'stdev_departure_time'
                                }
        
        self.reagg_args = {'avg_arrival_time':{agg_mean:{'value_field':'avg_arrival_time',
                                                         'n_field':'samples'}},
                           'avg_departure_time':{agg_mean:{'value_field':'avg_departure_time',
                                                           'n_field':'samples'}},
                           'stdev_arrival_time':{agg_std:{'mean_field':'avg_arrival_time',
                                                          'std_field':'stdev_arrival_time',
                                                          'n_field':'samples'}},
                           'stdev_departure_time':{agg_std:{'mean_field':'avg_departure_time',
                                                            'std_field':'stdev_departure_time',
                                                            'n_field':'samples'}},
                           'samples':    np.sum}
        
class stats():
    def __init__(self, apc_hdf, gtfs_paths, distributed=False, config_file=None, nodes=None, logger=None, depends=None):
        self.apc_path = apc_hdf
        self.apc_keys = get_keys(self.apc_path)
        self.calendar = None
        self.dow_by_service_id = None
        self.dow_count_by_service_id = None
        self.distributed = distributed
        self.config_file = config_file
        self.log = logger
        # APC Aggregations
#        self._default_groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL']
        self.sts_settings = stop_time_stats_settings()
#        self._default_stat_args = {'ARRIVAL_TIME':[meantime,stdtime,'size'],
#                                   'DEPARTURE_TIME',[meantime,stdtime]}
#        self._default_rename = {('ARRIVAL_TIME','meantime'):'avg_arrival_time',
#                                ('ARRIVAL_TIME','stdtime'):'stdev_arrival_time',
#                                ('ARRIVAL_TIME','size'):'samples',
#                                ('DEPARTURE_TIME','meantime'):'avg_departure_time',
#                                ('DEPARTURE_TIME','stdtime'):'stdev_departure_time'
#                                }
#        self._default_reagg_args = {'meantime':{agg_mean:{'value_field':'meantime',
#                                                          'n_field':'size'}},
#                                    'stdtime': {agg_std: {'mean_field':'meantime',
#                                                          'std_field':'stdtime',
#                                                          'n_field':'size'}},
#                                    'size':    np.sum}
#        self._default_reagg_args = {'avg_arrival_time':{agg_mean:{'value_field':'meantime',
#                                                          'n_field':'size'}},
#                                    'stdtime': {agg_std: {'mean_field':'meantime',
#                                                          'std_field':'stdtime',
#                                                          'n_field':'size'}},
#                                    'size':    np.sum}
        self.route_rebrand = {'8X':'8', '16X':'7X', '17':'57', '71':'7', '108':'25', '5L':'5R', '9L':'9R',
                              '14L':'14R', '28L':'28R', '38L':'38R', '71L':'7R'}
        
        self._apc_stop_time_stats = None
        
        # GTFS-STAT
        self.gtfs_to_apc = None 
        self.apc_to_gtfs = None
        self.route_stats = None
        self.trip_stats = None
        self.stop_time_stats = None
        
        self.gtfs_feeds = {}
        self.gtfs_paths = gtfs_paths
        if isinstance(self.gtfs_paths, str):
            self.gtfs_paths = [self.gtfs_paths]
        self._load_gtfs_feeds()
        
        sids = []
        self.log.info('getting service_id info from gtfs paths')
        for idx, gtfs_path in izip(range(len(self.gtfs_paths)), self.gtfs_paths):
            service_ids_by_date = ptg.read_service_ids_by_date(gtfs_path)
            service_ids_by_date = pd.DataFrame.from_dict(service_ids_by_date, orient='index').reset_index()
            service_ids_by_date.rename(columns={'index':'date', 0:'service_id'}, inplace=True)
            service_ids_by_date['file_idx'] = idx
            sids.append(service_ids_by_date)
        sids = pd.concat(sids)
        sids.loc[:,'weekday'] = sids['date'].map(lambda x: x.weekday())
        
        self.log.info('calculating date ranges and service_id stats')
        self.calendar = sids.groupby('file_idx').agg({'date':['min','max']})
        self.calendar.columns = ['start_date','end_date']
        self.dow_count_by_service_id = sids.pivot_table(index=['file_idx','service_id'],
                                                        columns=['weekday'], aggfunc='count')
        self.dow_count_by_service_id.columns = self.dow_count_by_service_id.columns.droplevel()
        self.dow_by_service_id = pd.notnull(self.dow_count_by_service_id) * 1
        dow_by_service_id = self.dow_by_service_id.reset_index()
        dow_by_file_idx = dow_by_service_id.loc[dow_by_service_id['service_id'].isin([1,'1'])].set_index('file_idx')
        for n, day in izip(range(7),['monday','tuesday','wednesday','thursday','friday','saturday','sunday']):
            self.calendar.loc[:,day] = dow_by_file_idx.loc[:,n]
            
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
        import utils
        from dispy_processing_utils import job_callback, load_pickle, dump_pickle, config, print_dispy_job_error
        from dispy_processing_utils import proc_stop_time_stats, proc_combine_stop_time_stats, filename_generator

        self.log.debug('reading config for distributed processing')
        self.config = config(self.config_file, nodes)
        self.log.debug('setting up job cluster')
        self._default_depends = [meantime, stdtime, load_pickle, dump_pickle, __file__, utils]
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
    
    def _load_gtfs_feeds(self, service_id=1):
        for i, gtfs_path in izip(range(len(self.gtfs_paths)), self.gtfs_paths):
            feed = ptg.feed(gtfs_path, view={'trips.txt':{'service_id':service_id}})
            self.gtfs_feeds[i] = feed
        return self.gtfs_feeds
    
    def apc_stop_time_stats(self, weekday=True, holiday=False, groupby=None, stat_args=None):
        '''
        Reads apc data from an h5 file and computes apc stop-time statistics.
        '''
        # TODO move shared code from _distributed and _sequential into here
        if self.distributed:
            self._apc_stop_time_stats_distributed(weekday, holiday, groupby, stat_args)
        else:
            self._apc_stop_time_stats_sequential(weekday, holiday, groupby, stat_args)
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
        groupby = self.sts_settings.groupby if groupby==None else groupby
        kwargs = self.sts_settings.reagg_args if kwargs=={} else kwargs
        self.log.debug('set groupby to %s' % (str(groupby)))
        self.log.debug('set kwargs to %s' % (str(kwargs)))
        
        grouped = self._apc_stop_time_stats.groupby(groupby)
        for column, arg in kwargs.iteritems():
            if isinstance(arg, dict):
                for aggfunc, kas in arg.iteritems():
                    #self.log.debug('column: %s' % (column))
                    #self.log.debug('aggfunc: %s' % (aggfunc))
                    #self.log.debug('kwargs: %s' % (str(kas)))
                    if len(arg) > 1:
                        columns.append((column,aggfunc.__name__))
                    else:
                        columns.append((column,))
                    try:
                        self.log.debug('grouped.agg({%s:%s}, %s)' % (column, aggfunc, kas))
                        agg = grouped.agg({column:aggfunc}, **kas)
                    except:
                        self.log.debug('grouped.apply(%s, %s)' % (aggfunc, kas))
                        agg = grouped.apply(aggfunc, **kas)
                    agg_dfs.append(agg)            
            elif isinstance(arg, list):
                for aggfunc in arg:
                    #self.log.debug('column: %s' % (column))
                    #self.log.debug('aggfunc: %s' % (str(aggfunc)))
                    if len(arg) > 1:
                        columns.append((column,aggfunc.__name__))
                    else:
                        columns.append((column,))
                    try:
                        agg = grouped.agg({column:aggfunc})
                    except:
                        agg = grouped.apply(aggfunc)
                    agg_dfs.append(agg)
            else:
                #self.log.debug('column: %s' % (column))
                #self.log.debug('aggfunc: %s' % (str(arg)))
                columns.append((column,))
                try:
                    agg = grouped.agg({column:arg})
                except:
                    agg = grouped.apply({column:arg})
                agg_dfs.append(agg)
        
        self.log.debug('found %s column names for %s aggregations' % (len(columns), len(agg_dfs)))
        #self.log.debug('columns: %s' % str(columns))
        mi = pd.MultiIndex.from_tuples(columns)
        df = pd.DataFrame(index=agg_dfs[0].index, columns=mi)    
        for col, agg in izip(mi, agg_dfs):
            df.loc[:,col] = agg
        #df.reset_index(inplace=True)
        self._apc_stop_time_stats = df.reset_index()
        return self._apc_stop_time_stats
    
    def _apc_stop_time_stats_sequential(self, weekday=True, holiday=False, groupby=None, stat_args=None):
        # apc data is stored by month (or possibly other chunks)
        groupby = self.sts_settings.groupby if groupby==None else groupby
        stat_args= self.sts_settings.stat_args if stat_args==None else stat_args
        chunks = []
        us_holidays = holidays.UnitedStates()
        
        for key in self.apc_keys:
            self.log.debug('reading file %s, key %s' % (self.apc_path, key))
            apc = pd.read_hdf(self.apc_path, key)
            
            self.log.debug('updating file_idx')
            for idx, row in self.calendar.iterrows():
                apc.loc[apc['DATE'].between(row['start_date'],row['end_date']),'file_idx'] = idx

            # create new attributes
            apc.loc[:,'weekday'] = apc['DATE'].map(lambda x: x.weekday())
            if weekday: 
                apc = apc.loc[apc['weekday'].isin([0,1,2,3,4])]
            if not holiday:
                apc = apc.loc[~apc['DATE'].map(lambda x: x in us_holidays)]
            stop_time_stats = apc.groupby(groupby).agg(stat_args)
            chunks.append(stop_time_stats)
            
        df = pd.concat(chunks)
        df.columns = df.columns.droplevel()
        df.reset_index(inplace=True)
        
        self._apc_stop_time_stats = df
        return self._apc_stop_time_stats
        
    def _apc_stop_time_stats_distributed(self, weekday=True, holiday=False, groupby=None, stat_args=None):
        # defaults
        groupby = self.sts_settings.groupby if groupby==None else groupby
        rename = self.sts_settings.rename
        stat_args= self.sts_settings.stat_args if stat_args==None else stat_args
        chunks = []
        wait_queue = {}
        to_merge = []
        i = 0
        us_holidays = holidays.UnitedStates()
        
        # iterate through keys.  apc data is stored by month (or possibly other chunks)
        for key in self.apc_keys:
            self.log.debug('reading file %s, key %s' % (self.apc_path, key))
            apc = pd.read_hdf(self.apc_path, key)
            
            # assign each record to the gtfs feed with corresponding date range
            self.log.debug('updating file_idx')
            for idx, row in self.calendar.iterrows():
                apc.loc[apc['DATE'].between(row['start_date'],row['end_date']),'file_idx'] = idx

            # create new attributes
            apc.loc[:,'weekday'] = apc['DATE'].map(lambda x: x.weekday())
            if weekday: 
                apc = apc.loc[apc['weekday'].isin([0,1,2,3,4])]
            if not holiday:
                apc = apc.loc[~apc['DATE'].map(lambda x: x in us_holidays)]
            ifile, ofile = self.fg.next(), self.fg.next()
            dump_pickle(ifile, apc)
            job = self.cluster.submit(ifile, ofile, groupby, stat_args)
            self.log.debug("submitting job %d" % (i))
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
                    self.log.debug("finished job %d" % jobid)
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
                self.log.debug("finished job %d" % jobid)
                pop_ids.append(jobid)
                to_merge.append(tempfile)
            except Exception as e:
                self.log.warn(e)
                print_dispy_job_error(job)
        for jobid in pop_ids:
            wait_queue.pop(jobid)
        
        for fname in to_merge:
            chunks.append(load_pickle(fname))
        
        df = pd.concat(chunks)
        #df.columns = df.columns.droplevel()
        # rename columns from MultiIndex that results from complex aggregation
        # to desired column names
        new_cols = []
        for c in df.columns:
            try:
                nc = rename[c]
                new_cols.append(nc)
            except Exception as e:
                self.log.debug('failed to rename column %s' % c)
                self.log.debug(e)
        df.columns = new_cols
        df.reset_index(inplace=True)
        #df.set_index('file_idx', inplace=True)
        #df.loc[:,'start_date'] = self.calendar['start_date']
        #df.loc[:,'end_date'] = self.calendar['end_date']
        #df.reset_index(inplace=True)
        #df.rename(columns={'file_idx':'service_id'}, inplace=True)
        df.loc[:,'service_id'] = df['file_idx']
        self._apc_stop_time_stats = df
        return self._apc_stop_time_stats
        
    def match_apc_to_gtfs(self):
        group_columns = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']
        agg_columns = ['SEQ','STOP_AVL','avg_arrival_time','stdev_arrival_time']
        first_stops = self._apc_stop_time_stats.groupby(group_columns)[agg_columns].first()
        first_stops.loc[:,'match_flag'] = 0
        file_idx, route_short_name, dir_ = None, None, None
        unfound_routes = set()
        for idx, feed in self.gtfs_feeds.iteritems():
            feed.stop_times.loc[:,'scheduled_arrival_time'] = feed.stop_times['arrival_time'].map(lambda x: dt.timedelta(seconds=x))
            feed.stop_times.loc[:,'scheduled_departure_time'] = feed.stop_times['departure_time'].map(lambda x: dt.timedelta(seconds=x))
        for idx, first_stop in first_stops.iterrows():
            if idx[0] != file_idx:
                file_idx = idx[0]
                feed = self.gtfs_feeds[file_idx]
                routes = feed.routes
            if idx[1] != route_short_name or idx[2] != dir_:
                route_short_name = idx[1]
                dir_ = idx[2]
                route = routes.loc[routes['route_short_name'].eq(route_short_name)]
                if len(route) == 0:
                    try:
                        route = routes.loc[routes['route_short_name'].eq(self.route_rebrand[route_short_name])]
                    except Exception as e:
                        self.log.debug(e)
                        continue
                if len(route) == 0:
                    unfound_routes.add(route_short_name)
                    continue
                trips = feed.trips.loc[feed.trips['route_id'].eq(route.iloc[0]['route_id']) & 
                                       feed.trips['direction_id'].eq(dir_)]
                stop_times = feed.stop_times.loc[feed.stop_times['trip_id'].isin(trips['trip_id'])]
            
            for numdev in [1,2,3]:
                start = first_stop['avg_arrival_time']-numdev*first_stop['stdev_arrival_time']
                stop = first_stop['avg_arrival_time']+numdev*first_stop['stdev_arrival_time']
                
                matches = stop_times.loc[stop_times['stop_id'].eq(str(first_stop['STOP_AVL'])) & 
                                         stop_times['scheduled_arrival_time'].between(start, stop)]
                if len(matches) > 1:
                    #self.log.debug('round multiple possible matches!')
                    #self.log.debug(str(matches))
                    matches.loc[:,'diff'] = (matches['scheduled_arrival_time'] - first_stop['avg_arrival_time']).map(lambda x: abs(x))
                    first_stops.loc[idx,'route_id'] = route.iloc[0]['route_id']
                    first_stops.loc[idx,'trip_id'] = matches.loc[matches['diff'].idxmin(),'trip_id']
                    first_stops.loc[idx,'match_flag'] = 1
                    break
                elif len(matches) == 0:
                    #self.log.debug('found no possible matches for:')
                    #self.log.debug(str(first_stop))
                    continue
                else:
                    first_stops.loc[idx,'route_id'] = route.iloc[0]['route_id']
                    first_stops.loc[idx,'trip_id'] = matches.iloc[0]['trip_id']
                    break
        #self.gtfs_to_apc = first_stops.reset_index().loc[:,['file_idx','route_id','trip_id','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']]
        self.apc_to_gtfs = first_stops.loc[:,['route_id','trip_id']]
        return first_stops
    
    def datetime_to_timedelta(d):
        try:
            return d - dt.datetime(d.year, d.month, d.day)
        except:
            return pd.NaT
    def make_stop_time_stats(self):
        if not isinstance(self.apc_to_gtfs, pd.DataFrame):
            self.log.debug('need to map gtfs to apc first!')
            return
        if not isinstance(self._apc_stop_time_stats, pd.DataFrame):
            self.log.debug('need to create apc_stop_time_stats first!')
            return
        stop_time_stats = self._apc_stop_time_stats.set_index(['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP'])
        stop_time_stats.loc[:,'route_id'] = np.nan
        stop_time_stats.loc[:,'trip_id'] = np.nan
        stop_time_stats.update(self.apc_to_gtfs)
        stop_time_stats.rename(columns={'STOP_AVL':'stop_id',
                                        'SEQ':'stop_sequence'}, inplace=True)
        stop_time_stats.loc[:,'scheduled_arrival_time'] = pd.NaT
        stop_time_stats.loc[:,'scheduled_departure_time'] = pd.NaT
        stop_time_stats.loc[:,'stop_id'] = stop_time_stats['stop_id'].astype('str')
        stop_time_stats = stop_time_stats.reset_index().set_index(['file_idx','trip_id','stop_sequence','stop_id'])
        #stop_time_stats.to_csv(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018Mar06.160558\stop_time_stats_provisional.csv')
        #stop_time_stats.to_hdf(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018Mar06.160558\stop_time_stats_provisional.h5','data')
        for idx, feed in self.gtfs_feeds.iteritems():
            stop_times = pd.DataFrame(feed.stop_times, copy=True)
            stop_times.loc[:,'file_idx'] = idx
            stop_times.set_index(['file_idx','trip_id','stop_sequence','stop_id'], inplace=True)
            stop_time_stats.update(stop_times)
        stop_time_stats.loc[:,'scheduled_arrival_time'] = stop_time_stats['scheduled_arrival_time'].map(lambda x: datetime_to_timedelta(x))
        stop_time_stats.loc[:,'scheduled_departure_time'] = stop_time_stats['scheduled_departure_time'].map(lambda x: datetime_to_timedelta(x))
        stop_time_stats = stop_time_stats.reset_index().loc[:,['service_id',
                                                               'trip_id',
                                                               'stop_sequence',
                                                               'stop_id',
                                                               'scheduled_arrival_time',
                                                               'scheduled_departure_time',
                                                               'avg_arrival_time',
                                                               'stdev_arrival_time',
                                                               'avg_departure_time',
                                                               'stdev_departure_time',
                                                               'samples']]
        self.stop_time_stats = stop_time_stats
        return stop_time_stats
