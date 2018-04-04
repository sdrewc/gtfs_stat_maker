__author__      = "Drew Cooper"

import sys, os, logging
import numpy as np
import pandas as pd
import datetime as dt
import partridge as ptg
from itertools import izip
sys.path.insert(0,os.path.dirname(os.path.realpath(__file__)))
from utils import get_keys, meantime, stdtime, agg_mean, agg_std, normalize_timedelta, datetime_to_seconds, datetime_to_timedelta, agg_trip_runtime, apply_calc_runtime, apply_calc_movetime, apply_diff
import holidays
    
class match_apc_to_gtfs_settings():
    pass

class stop_time_stats_settings():
    def __init__(self):
        # Set of unique identifiers for a route-stop in the APC data
        self.groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL']
        self.sortby = None
        # Aggregations based on the above groupby
        self.agg_args = {'ARRIVAL_TIME':[meantime,stdtime,'size'],
                          'DEPARTURE_TIME':[meantime,stdtime]}
        
        # Rename rules for aggregations to get to desired stop_time_stats names
        # The "from" is either: 
        #   1. the name of the original field that was aggregated, or
        #   2. a tuple of (original field name, aggregation function name)                           
        self.rename = {('ARRIVAL_TIME','meantime'):'avg_arrival_time',
                       ('ARRIVAL_TIME','stdtime'):'stdev_arrival_time',
                       ('ARRIVAL_TIME','size'):'samples',
                       ('DEPARTURE_TIME','meantime'):'avg_departure_time',
                       ('DEPARTURE_TIME','stdtime'):'stdev_departure_time'
                       }
        self.apply_args = None
        # Rules for reaggregating statistics.  The use case is when data has been
        # aggregated in chunks, and you want to aggregate those chunks together
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
                           
class stop_list_setting():
    def __init__(self):
        pass
    
class trip_list_settings():
    def __init__(self, source='apc'):
        '''
        Use stop times to calculate attributes for individual trips
        '''
        source = source.lower()
        if source not in ['apc','gtfs']:
            raise Exception('source must be "apc" or "gtfs", got %s instead' % source)
        if source=='apc':
            self.groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','DATE','TRIP']
            self.sortby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','DATE','TRIP','SEQ']
            self.agg_args = {'ARRIVAL_TIME':['first','last'],
                             'stopped_time':['sum']}
            
            self.rename = {('ARRIVAL_TIME','first'):'first_arrival_time',
                           ('ARRIVAL_TIME','last'):'last_arrival_time',
                           ('stopped_time','sum'):'observed_stopped_time'}
            
            self.apply_args = {'observed_runtime':apply_calc_runtime,
                               'observed_moving_time':apply_calc_movetime}
        elif source=='gtfs':
            self.groupby = ['file_idx','trip_id']
            self.sortby = ['file_idx','trip_id','stop_sequence']
            self.agg_args = {'arrival_time':['first'],
                             'departure_time':['last'],
                             'stopped_time':['sum']
                             }
            self.rename = {('arrival_time','first'):'first_arrival_time',
                           ('departure_time','last'):'last_arrival_time',
                           ('stopped_time','sum'):'scheduled_stopped_time'}
            
            self.apply_args = {'scheduled_runtime':[apply_diff,{'val1':'last_arrival_time','val2':'first_arrival_time'}],
                               'scheduled_moving_time':[apply_diff,{'val1':'scheduled_runtime','val2':'scheduled_stopped_time'}]}
class trip_stats_settings():
    def __init__(self):
        '''
        trip_stats_settings contains arguments that will be used to manipulate apc and gtfs
        data into the GTFS-STAT format.
        
        '''
        self.groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']
        self.sortby = None
        self.agg_args = {'observed_runtime':[pd.Series.mean,pd.Series.std],
                         'observed_stopped_time':[pd.Series.mean,pd.Series.std],
                         'observed_moving_time':[pd.Series.mean,pd.Series.std,'size']}        
        self.rename = {('observed_runtime','mean'):'avg_observed_runtime',
                       ('observed_runtime','std'):'stdev_observed_runtime',
                       ('observed_stopped_time','mean'):'avg_observed_stopped_time',
                       ('observed_stopped_time','std'):'stdev_observed_stopped_time',
                       ('observed_moving_time','mean'):'avg_observed_moving_time',
                       ('observed_moving_time','std'):'stdev_observed_moving_time',
                       ('observed_moving_time','size'):'samples',} 
        self.apply_args = None
        
class stats():
    def __init__(self, apc_hdf, gtfs_paths, distributed=False, config_file=None, nodes=None, logger=None, depends=None, tempdir=None):
        self.apc_path = apc_hdf
        self.apc_keys = get_keys(self.apc_path)
        self.calendar = None
        self.dow_by_service_id = None
        self.dow_count_by_service_id = None
        self.distributed = distributed
        self.config_file = config_file
        self.log = logger
        self.tempdir = r'C:\Temp' if tempdir == None else tempdir
        
        # APC Aggregations
        self.sts_settings = stop_time_stats_settings()
        self.tl_apc_settings = trip_list_settings(source='apc')
        self.tl_gtfs_settings = trip_list_settings(source='gtfs')
        self.ts_settings = trip_stats_settings()
        
        self.route_rebrand = {'8X':'8', '16X':'7X', '17':'57', '71':'7', '108':'25', '5L':'5R', '9L':'9R',
                              '14L':'14R', '28L':'28R', '38L':'38R', '71L':'7R'}
        
        self._apc_stop_time_stats = None
        self._apc_pickle_files = None
        self._apc_trip_stats = None
        
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
        self._default_depends = [meantime, stdtime, load_pickle, dump_pickle, __file__, utils]#, apply_calc_runtime, apply_calc_movetime
        self.depends = self._default_depends if depends==None else depends
        self.log.debug('setting up filename generator')
        self.fg = filename_generator(r'%s\tmp_gtfs_stat' % self.tempdir)
        self.log.debug('setting up globals')
        jobs_cond = threading.Condition()
        lower_bound = self.config.lower_bound
        upper_bound = self.config.upper_bound
        submit_queue = {}
        self.log.debug('done setting up')
    
    def _load_gtfs_feeds(self, service_id=1):
        for i, gtfs_path in izip(range(len(self.gtfs_paths)), self.gtfs_paths):
            feed = ptg.feed(gtfs_path, view={'trips.txt':{'service_id':service_id}})
            feed.trips.loc[:,'file_idx'] = i
            feed.stop_times.loc[:,'file_idx'] = i
            feed.stop_times.loc[:,'departure_time'] = feed.stop_times['departure_time'].map(lambda x: dt.timedelta(seconds=x))
            feed.stop_times.loc[:,'arrival_time'] = feed.stop_times['arrival_time'].map(lambda x: dt.timedelta(seconds=x))
            feed.stop_times.loc[:,'stopped_time'] = feed.stop_times['departure_time'] - feed.stop_times['arrival_time']
            self.gtfs_feeds[i] = feed
        return self.gtfs_feeds
        
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
                columns.append((column,))
                try:
                    agg = grouped.agg({column:arg})
                except:
                    agg = grouped.apply({column:arg})
                agg_dfs.append(agg)
        
        self.log.debug('found %s column names for %s aggregations' % (len(columns), len(agg_dfs)))
        mi = pd.MultiIndex.from_tuples(columns)
        df = pd.DataFrame(index=agg_dfs[0].index, columns=mi)    
        for col, agg in izip(mi, agg_dfs):
            df.loc[:,col] = agg
            
        self._apc_stop_time_stats = df#.reset_index()
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
                    matches.loc[:,'diff'] = (matches['scheduled_arrival_time'] - first_stop['avg_arrival_time']).map(lambda x: abs(x))
                    first_stops.loc[idx,'route_id'] = route.iloc[0]['route_id']
                    first_stops.loc[idx,'trip_id'] = matches.loc[matches['diff'].idxmin(),'trip_id']
                    first_stops.loc[idx,'match_flag'] = 1
                    break
                elif len(matches) == 0:
                    continue
                else:
                    first_stops.loc[idx,'route_id'] = route.iloc[0]['route_id']
                    first_stops.loc[idx,'trip_id'] = matches.iloc[0]['trip_id']
                    break
        self.apc_to_gtfs = first_stops.loc[:,['route_id','trip_id']]
        return first_stops
    
    def datetime_to_timedelta(d):
        try:
            return d - dt.datetime(d.year, d.month, d.day)
        except:
            return pd.NaT
    
    def make_stop_time_stats(self, weekday=True, holiday=True):
        '''
        Aggregates stop_time_stats into trip_stats        
        order of operations:
            1. for both apc and gtfs:
                1.1 calculate any stop-level statistics (i.e. dwell time)
                1.2 calculate trip-level statistics (i.e. run time)
                1.3 aggregate trip-level statistics across all observations
                1.4 if processed in chunks, reaggreagate individual chunks together
            2. combine apc and gtfs
            
        '''        
        # prep apc data if it has not already been done.
        if self._apc_pickle_files==None:
            self._prep_apc_pickle_files(weekday, holiday)
            
        apc_files = []
        for key in self.apc_keys:
            apc_files.append(self._apc_pickle_files[key])
        gtfs_trip_stat_files = []
        for idx, feed in self.gtfs_feeds.iteritems():
            ofile = self.fg.next()
            dump_pickle(ofile, feed.stop_times)
            gtfs_trip_stat_files.append(ofile)
        if self.distributed:
            self._apc_stop_time_stats = self._aggregate_and_apply(apc_files,
                                                                  groupby=self.sts_settings.groupby, 
                                                                  sortby=self.sts_settings.sortby, 
                                                                  rename=self.sts_settings.rename, 
                                                                  agg_args=self.sts_settings.agg_args, 
                                                                  apply_args=self.sts_settings.apply_args)
            #TODO update _aggregate_df to take place of reagg
            self.reagg_apc_stop_time_stats(groupby=self.sts_settings.groupby,
                                           **self.sts_settings.reagg_args)
#            self._apc_stop_time_stats = self._aggregate_df(data=self._apc_trip_list,
#                                                      groupby=self.ts_settings.groupby, 
#                                                      sortby=self.ts_settings.sortby, 
#                                                      rename=self.ts_settings.rename, 
#                                                      agg_args=self.ts_settings.agg_args, 
#                                                      apply_args=self.ts_settings.apply_args)

        else:
            pass
            #self._apc_trip_list_sequential(weekday, holiday, groupby, sortby, rename, agg_args, apply_args)
            #self._apc_trip_stats_sequential(weekday, holiday, groupby, sortby, rename, agg_args, apply_args)
        
        self.match_apc_to_gtfs()
        stop_time_stats = self._apc_stop_time_stats.set_index(['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP'])
        stop_time_stats.loc[:,'route_id'] = np.nan
        stop_time_stats.loc[:,'trip_id'] = np.nan
        stop_time_stats.update(self.apc_to_gtfs)
        stop_time_stats.rename(columns={'STOP_AVL':'stop_id',
                                        'SEQ':'stop_sequence'}, inplace=True)
        stop_time_stats.loc[:,'scheduled_arrival_time'] = pd.NaT
        stop_time_stats.loc[:,'scheduled_departure_time'] = pd.NaT
        stop_time_stats.loc[:,'stop_id'] = stop_time_stats['stop_id'].astype('str')
        stop_time_stats.set_index(['file_idx','trip_id','stop_sequence','stop_id'], inplace=True)
        
        for idx, feed in self.gtfs_feeds.iteritems():
            stop_times = pd.DataFrame(feed.stop_times, copy=True)
            stop_times.set_index(['file_idx','trip_id','stop_sequence','stop_id'], inplace=True)
            stop_time_stats.update(stop_times, overwrite=False)
            
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
    
    def make_stop_time_stats_old(self):
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

    def make_trip_stats(self, weekday=True, holiday=False):
        '''
        Aggregates stop_time_stats into trip_stats        
        order of operations:
            1. for both apc and gtfs:
                1.1 calculate any stop-level statistics (i.e. dwell time)
                1.2 calculate trip-level statistics (i.e. run time)
                1.3 aggregate trip-level statistics across all observations
                1.4 if processed in chunks, reaggreagate individual chunks together
            2. combine apc and gtfs
            
        '''        
        # prep apc data if it has not already been done.
        if self._apc_pickle_files==None:
            self._prep_apc_pickle_files(weekday, holiday)
            
        apc_files = []
        for key in self.apc_keys:
            apc_files.append(self._apc_pickle_files[key])
        gtfs_trip_stat_files = []
        for idx, feed in self.gtfs_feeds.iteritems():
            ofile = self.fg.next()
            dump_pickle(ofile, feed.stop_times)
            gtfs_trip_stat_files.append(ofile)
        if self.distributed:
            self._apc_trip_list = self._aggregate_and_apply(apc_files,
                                                            groupby=self.tl_apc_settings.groupby, 
                                                            sortby=self.tl_apc_settings.sortby, 
                                                            rename=self.tl_apc_settings.rename, 
                                                            agg_args=self.tl_apc_settings.agg_args, 
                                                            apply_args=self.tl_apc_settings.apply_args)
            self._apc_trip_stats = self._aggregate_df(data=self._apc_trip_list,
                                                      groupby=self.ts_settings.groupby, 
                                                      sortby=self.ts_settings.sortby, 
                                                      rename=self.ts_settings.rename, 
                                                      agg_args=self.ts_settings.agg_args, 
                                                      apply_args=self.ts_settings.apply_args)
            self._gtfs_trip_list = self._aggregate_and_apply(gtfs_trip_stat_files,
                                                             groupby=self.tl_gtfs_settings.groupby, 
                                                             sortby=self.tl_gtfs_settings.sortby, 
                                                             rename=self.tl_gtfs_settings.rename, 
                                                             agg_args=self.tl_gtfs_settings.agg_args, 
                                                             apply_args=self.tl_gtfs_settings.apply_args)
        else:
            pass
            #self._apc_trip_list_sequential(weekday, holiday, groupby, sortby, rename, agg_args, apply_args)
            #self._apc_trip_stats_sequential(weekday, holiday, groupby, sortby, rename, agg_args, apply_args)
        
        trip_stats = pd.DataFrame(self._apc_trip_stats, copy=True)
        self.log.debug(trip_stats.columns)
        trip_stats.loc[:,'route_id'] = np.nan
        trip_stats.loc[:,'trip_id'] = np.nan
        trip_stats.update(self.apc_to_gtfs)
        trip_stats.loc[:,'scheduled_runtime'] = pd.NaT
        trip_stats.loc[:,'scheduled_moving_time'] = pd.NaT
        trip_stats.loc[:,'scheduled_stopped_time'] = pd.NaT
        trip_stats = trip_stats.reset_index().set_index(['file_idx','trip_id'])
        gtfs_trips = self._gtfs_trip_list.reset_index().set_index(['file_idx','trip_id'])
        self.log.debug(trip_stats.head())
        trip_stats.update(gtfs_trips, overwrite=False)
        self.log.debug(trip_stats.head())
        # the update casts timedeltas to datetimes for some reason, so cast them back
        trip_stats.loc[:,'scheduled_runtime'] = trip_stats['scheduled_runtime'].map(lambda x: datetime_to_timedelta(x))
        trip_stats.loc[:,'scheduled_moving_time'] = trip_stats['scheduled_moving_time'].map(lambda x: datetime_to_timedelta(x))
        trip_stats.loc[:,'scheduled_stopped_time'] = trip_stats['scheduled_stopped_time'].map(lambda x: datetime_to_timedelta(x))
        self.log.debug(trip_stats.head())

        trip_stats = trip_stats.reset_index().loc[:,['file_idx','route_id','trip_id',
                                                     'scheduled_runtime',
                                                     'scheduled_moving_time',
                                                     'scheduled_stopped_time',
                                                     'avg_observed_runtime',
                                                     'stdev_observed_runtime',
                                                     'avg_observed_moving_time',
                                                     'stdev_observed_moving_time',
                                                     'avg_observed_stopped_time',
                                                     'stdev_observed_stopped_time']]

        self.trip_stats = trip_stats
        return trip_stats

    def _aggregate_df(self, data, groupby, sortby, rename, agg_args, apply_args):
        if sortby != None:
            data.sort_values(by=sortby, inplace=True)
        
        agg = data.groupby(groupby).agg(agg_args)
        
        if rename!=None:
            print "proc_aggregate.renaming columns"
            new_cols = []
            for c in agg.columns:
                try:
                    nc = rename[c]
                    new_cols.append(nc)
                except Exception as e:
                    print 'failed to rename column %s' % c
                    print e
            agg.columns = new_cols
            
        if apply_args!=None:
            if not isinstance(apply_args, dict):
                raise Exception(r'apply_args must be type (dict)')
            for fname, apply_func in apply_args.iteritems():
                print "proc_apply.applying.%s" % (apply_func.__name__)
                agg[fname] = agg.apply(apply_func, axis=1)
                
        return agg
    
    def _aggregate_and_apply(self, infiles, groupby, sortby, rename, agg_args, apply_args):
        '''
        takes a list of pickled dataframe files, and then aggregates them using groupby, 
        sortby, rename, and agg_args; then applies the apply_args.  Returns a single 
        dataframe
        '''
        from dispy_processing_utils import proc_aggregate, proc_apply
        
        # submit aggregate jobs
        self.log.debug('creating iterator for aggregation jobs')
        agg_iter = self._iter_aggregate_job(infiles, groupby, sortby, rename, agg_args)
        self.log.debug('distributing aggregate jobs')
        results = self._distribute(proc_aggregate, agg_iter)
        self.log.debug(results)
        
        if apply_args!=None:
            self.log.debug('creating iterator for apply jobs')
            apply_iter = self._iter_apply_job(results, apply_args)
            self.log.debug('distributing apply jobs')
            results = self._distribute(proc_apply, apply_iter)
            self.log.debug(results)
        dfs = []
        for result in results:
            df = load_pickle(result)
            dfs.append(df)
        agg = pd.concat(dfs)
        #self._apc_trip_list = apc_trip_list
        return agg

    # job argument iterators
    def _iter_aggregate_job(self, files, groupby, sortby, rename, agg_args):
        for ifile in files:
            ofile = self.fg.next()
            yield (ifile, ofile, groupby, sortby, rename, agg_args)
            
    def _iter_apply_job(self, files, apply_args, axis=1):
        for ifile in files:
            ofile = self.fg.next()
            yield (ifile, ofile, apply_args, axis)

    def _prep_apc_pickle_files(self, weekday, holiday):
        apc_pickle_files = {}
        us_holidays = holidays.UnitedStates()
        for key in self.apc_keys:
            self.log.debug('reading file %s, key %s' % (self.apc_path, key))
            apc = pd.read_hdf(self.apc_path, key)
            
            # assign each record to the gtfs feed with corresponding date range
            self.log.debug('updating file_idx')
            for idx, row in self.calendar.iterrows():
                apc.loc[apc['DATE'].between(row['start_date'],row['end_date']),'file_idx'] = idx
                
            # create new attributes
            apc.loc[:,'stopped_time'] = apc['DEPARTURE_TIME'] - apc['ARRIVAL_TIME']
            apc.loc[:,'weekday'] = apc['DATE'].map(lambda x: x.weekday())
            if weekday: 
                apc = apc.loc[apc['weekday'].isin([0,1,2,3,4])]
            if not holiday:
                apc = apc.loc[~apc['DATE'].map(lambda x: x in us_holidays)]
            ifile = self.fg.next()
            dump_pickle(ifile, apc)
            apc_pickle_files[key] = ifile
        self._apc_pickle_files = apc_pickle_files
        return apc_pickle_files
    
    def _distribute(self, process, job_iter, depends=[]):
        # set up cluster
        cluster = dispy.JobCluster(process, 
                                   callback=job_callback,
                                   depends=self.depends+depends,
                                   nodes=self.config.nodes,
                                   loglevel=logging.info,
                                   )
        wait_queue = {}
        results = []
        i = 1
        for args in job_iter:
            job = cluster.submit(*args)
            self.log.debug("submitting %s job %d" % (process.__name__, i))
            jobs_cond.acquire()
            job.id = i
            if job.status == dispy.DispyJob.Created or job.status == dispy.DispyJob.Running:
                submit_queue[i] = job
                # wait for queue to fall before lower bound before submitting another job
                if len(submit_queue) >= upper_bound:
                    while len(submit_queue) > lower_bound:
                        print "waiting"
                        jobs_cond.wait()
                        print "done waiting"
            jobs_cond.release()
            
            wait_queue[i] = job
            pop_ids = []
            for jobid, job in wait_queue.iteritems():
                if job.status == dispy.DispyJob.Finished:
                    self.log.debug("finished job %d" % jobid)
                    result = job.result
                    pop_ids.append(jobid)
                    results.append(result)
                    self.log.debug('jobid %d: %s' % (jobid, result))
                    self.log.debug('- %s' % job.stderr)
                    self.log.debug('- %s' % job.stdout)
                    self.log.debug('- %s' % job.exception)
                elif job.status in [dispy.DispyJob.Abandoned, dispy.DispyJob.Cancelled,
                                    dispy.DispyJob.Terminated]:
                    self.log.debug('- %s' % job.stderr)
                    self.log.debug('- %s' % job.stdout)
                    self.log.debug('- %s' % job.exception)
            for jobid in pop_ids:
                wait_queue.pop(jobid)

            i += 1
        pop_ids = []
        self.log.debug('cleaning up remaining jobs')
        for jobid, job in wait_queue.iteritems():
            try:
                result = job()
                self.log.debug("finished job %d" % jobid)
                pop_ids.append(jobid)
                results.append(result)
                self.log.debug('jobid %d: %s' % (jobid, result))
                self.log.debug('- %s' % job.stderr)
                self.log.debug('- %s' % job.stdout)
                self.log.debug('- %s' % job.exception)
                #print_dispy_job_error(job)
            except Exception as e:
                self.log.warn(e)
                #print_dispy_job_error(job)
                self.log.debug('- %s' % job.stderr)
                self.log.debug('- %s' % job.stdout)
                self.log.debug('- %s' % job.exception)
        for jobid in pop_ids:
            wait_queue.pop(jobid)
        cluster.close()
        return results