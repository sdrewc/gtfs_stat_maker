__author__      = "Drew Cooper"

import sys, os, logging
import numpy as np
import pandas as pd
import datetime as dt
import partridge as ptg
from itertools import izip
sys.path.insert(0,os.path.dirname(os.path.realpath(__file__)))
from utils import get_keys, meantime, stdtime, agg_mean, agg_std, normalize_timedelta, datetime_to_seconds, datetime_to_timedelta, agg_trip_runtime, apply_diff, str_to_timedelta, apply_time_periods, calc_headway, df_format_datetimes_as_str, weighted_mean, semidev
import holidays

class stop_time_stats_settings():
    def __init__(self):
        # Set of unique identifiers for a route-stop in the APC data
        self.groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','SEQ','STOP_AVL']
        self.sortby = None
        
        # Aggregations based on the above groupby
        self.agg_args = {'ARRIVAL_TIME':[meantime,stdtime,'size'],
                         'DEPARTURE_TIME':[meantime,stdtime],
                         }
        
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
                             'stopped_time':['sum'],
                             'ON':['sum'],
                             'OFF':['sum'],
                             'LOAD_DEP':['max','mean']}
            
            self.rename = {('ARRIVAL_TIME','first'):'observed_start_time',
                           ('ARRIVAL_TIME','last'):'observed_end_time',
                           ('stopped_time','sum'):'observed_stopped_time',
                           ('ON','sum'):'on',
                           ('OFF','sum'):'off',
                           ('LOAD_DEP','max'):'max_load',
                           ('LOAD_DEP','mean'):'avg_load'}
            
            self.apply_args = [{'observed_start_time':datetime_to_timedelta},
                               {'observed_end_time':datetime_to_timedelta},
                               {'observed_runtime':[apply_diff,{'val1':'observed_end_time','val2':'observed_start_time'}]},
                               {'observed_moving_time':[apply_diff,{'val1':'observed_runtime','val2':'observed_stopped_time'}]},
                               {'load_change':[apply_diff,{'val1':'on','val2':'off'}]}
                               ]
        elif source=='gtfs':
            self.groupby = ['file_idx','trip_id','direction_id']
            self.sortby = ['file_idx','trip_id','direction_id','stop_sequence']
            self.agg_args = {'scheduled_arrival_time':['first'],
                             'scheduled_departure_time':['last'],
                             'stopped_time':['sum']
                             }
            self.rename = {('scheduled_arrival_time','first'):'scheduled_start_time',
                           ('scheduled_departure_time','last'):'scheduled_end_time',
                           ('stopped_time','sum'):'scheduled_stopped_time'}
            
            self.apply_args = {'scheduled_runtime':[apply_diff,{'val1':'scheduled_end_time','val2':'scheduled_start_time'}],
                               'scheduled_moving_time':[apply_diff,{'val1':'scheduled_runtime','val2':'scheduled_stopped_time'}]}
class trip_stats_settings():
    def __init__(self):
        '''
        trip_stats_settings contains arguments that will be used to manipulate apc and gtfs
        data into the GTFS-STAT format.
        
        '''
        self.groupby = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']
        self.sortby = None
        self.agg_args = {'observed_start_time':[pd.Series.mean,pd.Series.std,semidev],
                         'observed_runtime':[pd.Series.mean,pd.Series.std,semidev],
                         'observed_stopped_time':[pd.Series.mean,pd.Series.std],
                         'on':['sum',pd.Series.mean,pd.Series.std],
                         'off':['sum',pd.Series.mean,pd.Series.std],
                         'max_load':[pd.Series.mean,pd.Series.std],
                         'avg_load':[pd.Series.mean,pd.Series.std],
                         'observed_moving_time':[pd.Series.mean,pd.Series.std,'size']}        
        self.rename = {('observed_start_time','mean'):'avg_observed_start_time',
                       ('observed_start_time','std'):'stdev_observed_start_time',
                       ('observed_start_time','semidev'):'semidev_observed_start_time',
                       ('observed_runtime','mean'):'avg_observed_runtime',
                       ('observed_runtime','std'):'stdev_observed_runtime',
                       ('observed_runtime','semidev'):'semidev_observed_runtime',
                       ('observed_stopped_time','mean'):'avg_observed_stopped_time',
                       ('observed_stopped_time','std'):'stdev_observed_stopped_time',
                       ('observed_moving_time','mean'):'avg_observed_moving_time',
                       ('observed_moving_time','std'):'stdev_observed_moving_time',
                       ('on','sum'):'total_boardings',
                       ('on','mean'):'avg_boardings',
                       ('on','std'):'stdev_boardings',
                       ('off','sum'):'total_alightings',
                       ('off','mean'):'avg_alightings',
                       ('off','std'):'stdev_alightings',
                       ('max_load','mean'):'avg_max_load',
                       ('max_load','std'):'stdev_max_load',
                       ('avg_load','mean'):'avg_load',
                       ('avg_load','std'):'stdev_load',
                       ('observed_moving_time','size'):'samples',} 
        self.apply_args = None
        
class route_stats_settings():
    def __init__(self, source='apc', timeperiods=None):
        if source=='apc':
            self.groupby = ['route_id','route_short_name','service_id','direction_id','timeperiod_id']
            self.sortby = ['route_id','route_short_name','service_id','direction_id','scheduled_start_time','timeperiod_id']
            self.timeperiods = timeperiods #TODO
            self.agg_args = {'scheduled_start_time':[calc_headway],
                             'scheduled_runtime':[pd.Series.mean,pd.Series.std],
                             'scheduled_stopped_time':[pd.Series.mean,pd.Series.std],
                             'scheduled_moving_time':[pd.Series.mean,pd.Series.std],
                             'avg_boardings':['sum'],
                             'avg_alightings':['sum'],
                             #'max_lod':['mean','min','max'],
                             #'observed_start_time':[calc_headway],
                             #'observed_runtime':[pd.Series.mean,pd.Series.std],
                             'avg_observed_runtime':[pd.Series.mean],
                             #'observed_stopped_time':[pd.Series.mean,pd.Series.std],
                             'avg_observed_stopped_time':[pd.Series.mean],
                             #'observed_moving_time':[pd.Series.mean,pd.Series.std,'size']
                             'avg_observed_moving_time':[pd.Series.mean,'size']
                             }        
            self.rename = {('scheduled_start_time','calc_headway'):'avg_scheduled_headway',
                           ('scheduled_runtime','mean'):'avg_scheduled_runtime',
                           ('scheduled_runtime','std'):'stdev_scheduled_runtime',
                           ('scheduled_stopped_time','mean'):'avg_scheduled_stopped_time',
                           ('scheduled_stopped_time','std'):'stdev_scheduled_stopped_time',
                           ('scheduled_moving_time','mean'):'avg_scheduled_moving_time',
                           ('scheduled_moving_time','std'):'stdev_scheduled_moving_time',
                           ('avg_boardings','sum'):'avg_boardings',
                           #('on','std'):'stdev_boardings',
                           ('avg_alightings','sum'):'avg_alightings',
                           #('off','std'):'stdev_alightings',
                           #('observed_start_time','calc_headway'):'avg_observed_headway',
                           ('avg_observed_runtime','mean'):'avg_observed_runtime',
                           #('observed_runtime','std'):'stdev_observed_runtime',
                           ('avg_observed_stopped_time','mean'):'avg_observed_stopped_time',
                           #('observed_stopped_time','std'):'stdev_observed_stopped_time',
                           ('avg_observed_moving_time','mean'):'avg_observed_moving_time',
                           #('observed_moving_time','std'):'stdev_observed_moving_time',
                           ('avg_observed_moving_time','size'):'samples',} 
            self.apply_args = None
        if source=='gtfs':
            self.groupby = ['route_id','route_short_name','service_id','direction_id','timeperiod_id']
            self.sortby = ['route_id','route_short_name','service_id','direction_id','scheduled_start_time','timeperiod_id']
            self.timeperiods = timeperiods #TODO
            self.agg_args = {'scheduled_start_time':[calc_headway],
                             'scheduled_runtime':[pd.Series.mean,pd.Series.std],
                             'scheduled_stopped_time':[pd.Series.mean,pd.Series.std],
                             'scheduled_moving_time':[pd.Series.mean,pd.Series.std],
                             #'observed_start_time':[calc_headway],
                             #'observed_runtime':[pd.Series.mean,pd.Series.std],
                             #'observed_stopped_time':[pd.Series.mean,pd.Series.std],
                             #'observed_moving_time':[pd.Series.mean,pd.Series.std,'size']}   
                             }
            self.rename = {('scheduled_start_time','calc_headway'):'avg_scheduled_headway',
                           ('scheduled_runtime','mean'):'avg_scheduled_runtime',
                           ('scheduled_runtime','std'):'stdev_scheduled_runtime',
                           ('scheduled_stopped_time','mean'):'avg_scheduled_stopped_time',
                           ('scheduled_stopped_time','std'):'stdev_scheduled_stopped_time',
                           ('scheduled_moving_time','mean'):'avg_scheduled_moving_time',
                           ('scheduled_moving_time','std'):'stdev_scheduled_moving_time',
                           #('observed_start_time','calc_headway'):'observed_avg_headway',
                           #('observed_runtime','mean'):'avg_observed_runtime',
                           #('observed_runtime','std'):'stdev_observed_runtime',
                           #('observed_stopped_time','mean'):'avg_observed_stopped_time',
                           #('observed_stopped_time','std'):'stdev_observed_stopped_time',
                           #('observed_moving_time','mean'):'avg_observed_moving_time',
                           #('observed_moving_time','std'):'stdev_observed_moving_time',
                           #('observed_moving_time','size'):'samples',} 
                           }
            self.apply_args = None

class group_trip_list_settings():
    def __init__(self, source='apc'):
        source = source.lower()
        if source not in ['apc','gtfs']:
            raise Exception('source must be "apc" or "gtfs", got %s instead' % source)
        if source=='apc':
            self.groupby = ['file_idx','group_id','route_id','route_short_name',
                            'direction_id','trip_id','DATE',
                            ]
            self.sortby = ['file_idx','group_id','route_id','route_short_name',
                            'direction_id','trip_id','DATE','stop_sequence']
            self.agg_args = {'ARRIVAL_TIME':['first','last'],
                             'stopped_time':['sum'],
                             'ON':['sum'],
                             'OFF':['sum'],
                             'LOAD_DEP':['max','mean']}
            
            self.rename = {('ARRIVAL_TIME','first'):'observed_start_time',
                           ('ARRIVAL_TIME','last'):'observed_end_time',
                           ('stopped_time','sum'):'observed_stopped_time',
                           ('ON','sum'):'on',
                           ('OFF','sum'):'off',
                           ('LOAD_DEP','max'):'max_load',
                           ('LOAD_DEP','mean'):'avg_load'}
            
            self.apply_args = [{'observed_start_time':datetime_to_timedelta},
                               {'observed_end_time':datetime_to_timedelta},
                               {'observed_runtime':[apply_diff,{'val1':'observed_end_time','val2':'observed_start_time'}]},
                               {'observed_moving_time':[apply_diff,{'val1':'observed_runtime','val2':'observed_stopped_time'}]},
                               {'load_change':[apply_diff,{'val1':'on','val2':'off'}]}
                               ]
        elif source=='gtfs':
            self.groupby = ['file_idx','group_id','trip_id','direction_id']
            self.sortby = ['file_idx','group_id','trip_id','direction_id','stop_sequence']
            self.agg_args = {'scheduled_arrival_time':['first'],
                             'scheduled_departure_time':['last'],
                             'stopped_time':['sum']
                             }
            self.rename = {('scheduled_arrival_time','first'):'scheduled_start_time',
                           ('scheduled_departure_time','last'):'scheduled_end_time',
                           ('stopped_time','sum'):'scheduled_stopped_time'}
            
            self.apply_args = {'scheduled_runtime':[apply_diff,{'val1':'scheduled_end_time','val2':'scheduled_start_time'}],
                               'scheduled_moving_time':[apply_diff,{'val1':'scheduled_runtime','val2':'scheduled_stopped_time'}]}
        
class group_stats_settings():
    def __init__(self):
        # Set of unique identifiers for a group in the APC data
        self.groupby = ['group_id','service_id','direction_id','timeperiod_id']
        self.sortby = ['group_id','service_id','direction_id','scheduled_start_time','timeperiod_id']
        
        # Aggregations based on the above groupby
        self.agg_args = {'scheduled_start_time':[calc_headway],
                         'scheduled_runtime':[pd.Series.mean,pd.Series.std],
                         'scheduled_stopped_time':[pd.Series.mean,pd.Series.std],
                         'scheduled_moving_time':[pd.Series.mean,pd.Series.std],
                         #'observed_start_time':[calc_headway],
                         'observed_runtime':[pd.Series.mean,pd.Series.std],
                         'observed_stopped_time':[pd.Series.mean,pd.Series.std],
                         'observed_moving_time':[pd.Series.mean,pd.Series.std,'size']}   
        
 
        self.rename = {('scheduled_start_time','calc_headway'):'scheduled_avg_headway',
                       ('scheduled_runtime','mean'):'avg_scheduled_runtime',
                       ('scheduled_runtime','std'):'stdev_scheduled_runtime',
                       ('scheduled_stopped_time','mean'):'avg_scheduled_stopped_time',
                       ('scheduled_stopped_time','std'):'stdev_scheduled_stopped_time',
                       ('scheduled_moving_time','mean'):'avg_scheduled_moving_time',
                       ('scheduled_moving_time','std'):'stdev_scheduled_moving_time',
                       #('observed_start_time','calc_headway'):'observed_avg_headway',
                       ('observed_runtime','mean'):'avg_observed_runtime',
                       ('observed_runtime','std'):'stdev_observed_runtime',
                       ('observed_stopped_time','mean'):'avg_observed_stopped_time',
                       ('observed_stopped_time','std'):'stdev_observed_stopped_time',
                       ('observed_moving_time','mean'):'avg_observed_moving_time',
                       ('observed_moving_time','std'):'stdev_observed_moving_time',
                       ('observed_moving_time','size'):'samples',} 
        self.apply_args = None
        
class stats():
    def __init__(self, apc_hdf, gtfs_paths, distributed=False, config_file=None, nodes=None, logger=None, depends=None, tempdir=None, timeperiods=None, groups=None):
        self.apc_path = apc_hdf
        self.apc_keys = get_keys(self.apc_path)
        self.calendar = None
        self._default_timeperiods = {'ea':['03:00:00','06:00:00'],
                                     'am':['06:00:00','09:00:00'],
                                     'md':['09:00:00','15:30:00'],
                                     'pm':['15:30:00','18:30:00'],
                                     'ev':['18:30:00','27:00:00']}
        self.timeperiods = self.set_timeperiods(timeperiods) if timeperiods else self.set_timeperiods(self._default_timeperiods)
        
        #print self.timeperiods
        #self.log.debug(self.timeperiods)
        #self.timeperiods.to_csv(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018May14.135146\timeperiods.txt')
        
        self.dow_by_service_id = None
        self.dow_count_by_service_id = None
        self.distributed = distributed
        self.config_file = config_file
        self.log = logger
        self.tempdir = r'C:\Temp' if tempdir == None else tempdir
        
        if isinstance(groups, pd.DataFrame):
            self.groups = groups
        elif isinstance(groups, str):
            self.read_groups(groups)
        else:
            self.groups = None
        
        # APC Aggregations
        self.sts_settings = stop_time_stats_settings()
        self.tl_apc_settings = trip_list_settings(source='apc')
        self.tl_gtfs_settings = trip_list_settings(source='gtfs')
        self.gtl_apc_settings = group_trip_list_settings(source='apc')
        self.gtl_gtfs_settings = group_trip_list_settings(source='gtfs')
        self.ts_settings = trip_stats_settings()
        self.rs_settings = route_stats_settings()
        self.rs_gtfs_settings = route_stats_settings(source='gtfs')
        self.gs_settings = group_stats_settings()
        
        self.route_rebrand = {'8X':'8', '16X':'7X', '17':'57', '71':'7', '108':'25', '5L':'5R', '9L':'9R',
                              '14L':'14R', '28L':'28R', '38L':'38R', '71L':'7R'}
        
        # GTFS-STAT INTERNALS
        # Internals have all necessary statitcs for the given aggregation 
        # level, even though they may be used in different output files.
        # For example, _trip_stats will contain both the operational performane
        # statistics for GTFS-STAT's trip_stats.txt and ridership statistics
        # for GTFS-Ride's ridership.txt.
        # Internals have datetime/numeric formats used in calculations
        self._apc_stop_time_stats = None
        self._apc_pickle_files = None
        self._apc_trip_stats = None
        self._apc_trip_list = None
        self._gtfs_trip_list = None
        self._trip_list = None
        self._group_trip_list = None
        self.group_stop_time_index = None
        
        self._group_stats = None
        self._route_stats = None
        self._trip_stats = None
        self._stop_time_stats = None
        
        # GTFS-STAT EXTERNALS
        # Externals have str/numeric formats used for writing
        self.apc_to_gtfs = None
        self.group_stats = None
        self.route_stats = None
        self.trip_stats = None
        self.stop_time_stats = None
        
        # GTFS-Ride EXTERNALS
        self.ridership = None
        
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
        
    def read_groups(self, groups):
        groups = pd.read_csv(groups)
        for f in ['from_node_set','to_node_set','thru_node_set']:
            groups.loc[:,f] = groups[f].map(lambda x: self.parse_node_set(x))
        self.groups = groups
            
    def parse_node_set(self, x):
        return str(x).replace('[','').replace(']','').split(',')
    
    def set_timeperiods(self, timeperiods):
        '''
        Sets timeperiods from a dict or dataframe.  If
        a dict is passed, the key must be the timeperiod name
        and the value must be a 2-item tuple or list of (start-time, end-time)
        as string values in HH:MM:SS, or datetime.time
        '''
        if isinstance(timeperiods, dict):
            timeperiods = pd.DataFrame.from_dict(timeperiods, orient='index')
            timeperiods.index.name='timeperiod_id'
            timeperiods.columns=['timeperiod_start_time','timeperiod_end_time']
            timeperiods.reset_index(inplace=True)
        elif isinstance(timeperiods, pd.DataFrame):
            rename = {'id':'timeperiod_id',
                      'start_time':'timeperiod_start_time',
                      'end_time':'timeperiod_end_time'}
            if len(timeperiods.columns) < 3:
                timeperiods.reset_index(inplace=True)
            timeperiods.rename(columns=rename, inplace=True)
            if 'timeperiod_id' not in timeperiods.columns or \
            'timeperiod_start_time' not in timeperiods.columns or \
            'timeperiod_end_time' not in timeperiods.columns:
                timeperiods.columns=['timeperiod_id','timeperiod_start_time','timeperiod_end_time']
            
        timeperiods.loc[:,'timeperiod_start_time'] = timeperiods['timeperiod_start_time'].map(lambda x: str_to_timedelta(x))
        timeperiods.loc[:,'timeperiod_end_time'] = timeperiods['timeperiod_end_time'].map(lambda x: str_to_timedelta(x))
        self.timeperiods = timeperiods
        return timeperiods
                    
            
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
        has_thru = 'thru_node_set' in self.groups.columns
        all_group_trips = []
        for file_idx, gtfs_path in izip(range(len(self.gtfs_paths)), self.gtfs_paths):
            feed = ptg.feed(gtfs_path, view={'trips.txt':{'service_id':service_id}})
            feed.trips.loc[:,'file_idx'] = file_idx
            feed.stop_times.loc[:,'file_idx'] = file_idx
            feed.stop_times.loc[:,'scheduled_departure_time'] = feed.stop_times['departure_time'].map(lambda x: dt.timedelta(seconds=x))
            feed.stop_times.loc[:,'scheduled_arrival_time'] = feed.stop_times['arrival_time'].map(lambda x: dt.timedelta(seconds=x))
            feed.stop_times.loc[:,'stopped_time'] = feed.stop_times['scheduled_departure_time'] - feed.stop_times['scheduled_arrival_time']
            
            feed.trips.set_index(['trip_id'], inplace=True)
            feed.stop_times.set_index(['trip_id'], inplace=True)
            feed.stop_times.loc[:,'direction_id'] = np.nan
            feed.stop_times.update(feed.trips.loc[:,['direction_id']], overwrite=False)
            feed.trips.reset_index(inplace=True)
            feed.stop_times.reset_index(inplace=True)
            
            # identify the trip_ids in this file associated with each group
            if isinstance(self.groups, pd.DataFrame):
                self.log.debug('identifying groups for file %d...' % file_idx)
                for idx, row in self.groups.iterrows():
                    from_node_set = row['from_node_set'] if isinstance(row['from_node_set'], list) else [row['from_node_set']]
                    to_node_set = row['to_node_set'] if isinstance(row['to_node_set'], list) else [row['to_node_set']]
                    
                    if has_thru:
                        thru_node_set = row['thru_node_set'] if isinstance(row['thru_node_set'], list) else [row['thru_node_set']]
                        #self.log.debug(thru_node_set)
                    
                    # get the stop times with a node in the [from_node_set]
                    nodes = feed.stop_times.loc[feed.stop_times['stop_id'].isin(from_node_set),['trip_id','stop_sequence']]
                    nodes.rename(columns={'stop_sequence':'from_node_seq'}, inplace=True)
                    nodes.set_index('trip_id', inplace=True)
                    
                    # of these nodes, set the to_node_seq if it has a node in [to_node_set]
                    nodes.loc[:,'to_node_seq'] = feed.stop_times.loc[feed.stop_times['stop_id'].isin(to_node_set),].set_index('trip_id')['stop_sequence']
                    #self.log.debug(nodes)
                    #nodes.loc[:,'file_idx'] = file_idx
                    nodes.loc[:,'group_id'] = row['group_id']
                    
                    if has_thru and pd.notnull(row['thru_node_set']):
                        nodes.loc[:,'thru_node_seq'] = feed.stop_times.loc[feed.stop_times['stop_id'].isin(thru_node_set),].set_index('trip_id')['stop_sequence']
                        nodes.dropna(subset=['from_node_seq','to_node_seq','thru_node_seq'])
                        nodes = nodes.loc[nodes['from_node_seq'].lt(nodes['thru_node_seq']) & nodes['thru_node_seq'].lt(nodes['to_node_seq'])]
                    else:
                        nodes.dropna(subset=['from_node_seq','to_node_seq'])    
                        nodes = nodes.loc[nodes['from_node_seq'].lt(nodes['to_node_seq'])]
                    
                    for trip_id, n in nodes.iterrows():
                        #self.log.debug('setting group_id on stop_times for file %d and group %s' % (file_idx, row['group_id']))
                        #feed.stop_times.loc[feed.stop_times['trip_id'].eq(trip_id) & feed.stop_times['stop_sequence'].between(n['from_node_seq'],n['to_node_seq']), 'group_id'] = row['group_id']
                        gtrips = feed.stop_times.loc[feed.stop_times['trip_id'].eq(trip_id) & 
                                                     feed.stop_times['stop_sequence'].between(n['from_node_seq'],n['to_node_seq']), 
                                                     ['file_idx','trip_id','stop_id','stop_sequence']]
                        gtrips.loc[:,'group_id'] = row['group_id']
                        all_group_trips.append(gtrips)
            
                self._group_stop_time_index = pd.concat(all_group_trips)#.set_index(['trip_id','stop_id','stop_sequence'])
                
            self.gtfs_feeds[file_idx] = feed
        return self.gtfs_feeds
        
    def _prep_apc_pickle_files(self, weekday, holiday):
        apc_pickle_files = {}
        us_holidays = holidays.UnitedStates()
        
        apc_pickle_files = {}
        for idx, row in self.calendar.iterrows():
            apc_pickle_files[idx] = self.fg.next()
            if os.path.exists(apc_pickle_files[idx]):
                os.remove(apc_pickle_files[idx])
            
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
                
            grouped = apc.groupby('file_idx')
            for idx, group in grouped:
                ifile = apc_pickle_files[idx]
                if os.path.exists(ifile):
                    pre_apc = load_pickle(ifile)
                    group = pre_apc.append(group)
                
                dump_pickle(ifile, group)
        self._apc_pickle_files = apc_pickle_files
        return apc_pickle_files
            
    def match_apc_to_gtfs(self):
        group_columns = ['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP']
        agg_columns = ['SEQ','STOP_AVL','avg_arrival_time','stdev_arrival_time']
        first_stops = self._apc_stop_time_stats.reset_index().groupby(group_columns)[agg_columns].first()
        first_stops.loc[:,'match_flag'] = 0
        file_idx, route_short_name, dir_ = None, None, None
        unfound_routes = set()

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
                    first_stops.loc[idx,'route_short_name'] = route_short_name
                    first_stops.loc[idx,'trip_id'] = matches.loc[matches['diff'].idxmin(),'trip_id']
                    first_stops.loc[idx,'direction_id'] = dir_
                    first_stops.loc[idx,'match_flag'] = 1
                    break
                elif len(matches) == 0:
                    continue
                else:
                    first_stops.loc[idx,'route_id'] = route.iloc[0]['route_id']
                    first_stops.loc[idx,'route_short_name'] = route_short_name
                    first_stops.loc[idx,'trip_id'] = matches.iloc[0]['trip_id']
                    first_stops.loc[idx,'direction_id'] = dir_
                    break
        self.apc_to_gtfs = first_stops.loc[:,['route_id','route_short_name','trip_id','direction_id']]
        #self.apc_to_gtfs = pd.DataFrame(self._apc_to_gtfs, copy=True)
        return self.apc_to_gtfs
    
    def make_stop_time_stats(self, weekday=True, holiday=False):
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
        for key, apc_file in self._apc_pickle_files.iteritems():
            apc_files.append(apc_file)
            
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

        else:
            self._apc_stop_time_stats = self._aggregate_df(apc_files,
                                                           groupby=self.sts_settings.groupby, 
                                                           sortby=self.sts_settings.sortby, 
                                                           rename=self.sts_settings.rename, 
                                                           agg_args=self.sts_settings.agg_args, 
                                                           apply_args=self.sts_settings.apply_args)
        
        self.match_apc_to_gtfs()
        stop_time_stats = self._apc_stop_time_stats.reset_index().set_index(['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP'])
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

        self._stop_time_stats = stop_time_stats
        self.stop_time_stats = df_format_datetimes_as_str(self._stop_time_stats)
        return
        
    def make_trip_list(self, weekday=True, holiday=False):
        self.log.debug("making trip_list")
        # prep apc data if it has not already been done.
        if self._apc_pickle_files==None:
            self._prep_apc_pickle_files(weekday, holiday)
            
        apc_files = []
        for key, apc_file in self._apc_pickle_files.iteritems():
            apc_files.append(apc_file)
        gtfs_trip_stat_files = []
        for idx, feed in self.gtfs_feeds.iteritems():
            ofile = self.fg.next()
            dump_pickle(ofile, feed.stop_times)
            gtfs_trip_stat_files.append(ofile)
        if self.distributed:
            self.log.debug('distributing apc_trip_list')
            self._apc_trip_list = self._aggregate_and_apply(apc_files,
                                                            groupby=self.tl_apc_settings.groupby, 
                                                            sortby=self.tl_apc_settings.sortby, 
                                                            rename=self.tl_apc_settings.rename, 
                                                            agg_args=self.tl_apc_settings.agg_args, 
                                                            apply_args=self.tl_apc_settings.apply_args)
            self.log.debug('distributing gtfs_trip_list')
            self._gtfs_trip_list = self._aggregate_and_apply(gtfs_trip_stat_files,
                                                             groupby=self.tl_gtfs_settings.groupby, 
                                                             sortby=self.tl_gtfs_settings.sortby, 
                                                             rename=self.tl_gtfs_settings.rename, 
                                                             agg_args=self.tl_gtfs_settings.agg_args, 
                                                             apply_args=self.tl_gtfs_settings.apply_args)
        else:
            self._apc_trip_list = self._aggregate_df(apc_files,
                                                     groupby=self.tl_apc_settings.groupby, 
                                                     sortby=self.tl_apc_settings.sortby, 
                                                     rename=self.tl_apc_settings.rename, 
                                                     agg_args=self.tl_apc_settings.agg_args, 
                                                     apply_args=self.tl_apc_settings.apply_args)
            self._gtfs_trip_list = self._aggregate_df(gtfs_trip_stat_files,
                                                      groupby=self.tl_gtfs_settings.groupby, 
                                                      sortby=self.tl_gtfs_settings.sortby, 
                                                      rename=self.tl_gtfs_settings.rename, 
                                                      agg_args=self.tl_gtfs_settings.agg_args, 
                                                      apply_args=self.tl_gtfs_settings.apply_args)
           
        trip_list = pd.DataFrame(self._apc_trip_list, copy=True)
        trip_list.loc[:,'route_id'] = np.nan
        trip_list.loc[:,'route_short_name'] = np.nan
        trip_list.loc[:,'trip_id'] = np.nan
        trip_list.loc[:,'direction_id'] = np.nan
        trip_list = trip_list.reset_index().set_index(self.apc_to_gtfs.index.names)
        trip_list.update(self.apc_to_gtfs)
        trip_list.loc[:,'scheduled_start_time'] = pd.NaT
        trip_list.loc[:,'scheduled_end_time'] = pd.NaT
        trip_list.loc[:,'scheduled_runtime'] = pd.NaT
        trip_list.loc[:,'scheduled_moving_time'] = pd.NaT
        trip_list.loc[:,'scheduled_stopped_time'] = pd.NaT
        trip_list = trip_list.reset_index().set_index(['file_idx','trip_id','direction_id'])
        
        gtfs_trips = self._gtfs_trip_list.reset_index().set_index(['file_idx','trip_id','direction_id'])
        trip_list.update(gtfs_trips, overwrite=False)
        
        # the update casts timedeltas to datetimes for some reason, so cast them back
        trip_list.reset_index(inplace=True)
        trip_list.loc[:,'scheduled_start_time'] = trip_list['scheduled_start_time'].map(lambda x: datetime_to_timedelta(x))
        trip_list.loc[:,'scheduled_runtime'] = trip_list['scheduled_runtime'].map(lambda x: datetime_to_timedelta(x))
        trip_list.loc[:,'scheduled_moving_time'] = trip_list['scheduled_moving_time'].map(lambda x: datetime_to_timedelta(x))
        trip_list.loc[:,'scheduled_stopped_time'] = trip_list['scheduled_stopped_time'].map(lambda x: datetime_to_timedelta(x))
        
        # also cast this one to timedelta
        trip_list.loc[:,'observed_start_time'] = trip_list['observed_start_time'].map(lambda x: datetime_to_timedelta(x))
        trip_list.loc[:,'timeperiod_id'] = apply_time_periods(trip_list['scheduled_start_time'], self.timeperiods)
        
        trip_list.rename(columns={'file_idx':'service_id'}, inplace=True)
        
        trip_list = trip_list.loc[:,['service_id','route_id','route_short_name',
                                     'trip_id','direction_id','timeperiod_id',
                                     'scheduled_start_time',
                                     'scheduled_runtime',
                                     'scheduled_moving_time',
                                     'scheduled_stopped_time',
                                     'observed_start_time',
                                     'observed_runtime',
                                     'observed_moving_time',
                                     'observed_stopped_time',
                                     'on',
                                     'off',
                                     'max_load',
                                     'avg_load'
                                     ]]
        
        self._trip_list = trip_list
        self.log.debug("done making trip_list")
        return
    
    def make_group_trip_list(self, weekday=True, holiday=False):
        # make_stop_time_stats must be run first, so that self.apc_to_gtfs exists
        # trip_id,stop_id,stop_sequence ... 'file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP'
        
        # merge the filtered stop_times (self._group_stop_time_index) with apc mapping (apc_to_gtfs)
        group_apc = pd.merge(self._group_stop_time_index.set_index(['file_idx','trip_id']),
                             self.apc_to_gtfs.reset_index().set_index(['file_idx','trip_id']),
                             left_index=True,
                             right_index=True)
        group_apc.reset_index(inplace=True)
        group_apc['stop_id'] = group_apc['stop_id'].astype(np.int64)
        group_apc['stop_sequence'] = group_apc['stop_sequence'].astype(np.int64)
        apc_files = []
        
        self.log.debug('GROUP_APC')
        self.log.debug(group_apc.dtypes)
        
        # merge apc records with filtered stop_times
        for key, apc_file in self._apc_pickle_files.iteritems():
            apc = load_pickle(apc_file)
            self.log.debug('LOADED APC')
            self.log.debug(apc.dtypes)
            new_apc = pd.merge(group_apc, apc, 
                               left_on=['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','stop_id','stop_sequence'],
                               right_on=['file_idx','ROUTE_SHORT_NAME','DIR','PATTCODE','TRIP','STOP_AVL','SEQ'],
                               how='left')
            self.log.debug('NEW_APC')
            self.log.debug(new_apc.dtypes)
            new_apc_file = self.fg.next()
            apc_files.append(new_apc_file)
            dump_pickle(new_apc_file,new_apc)
        
        gtfs_files = []    
        
        # TODO: is this necessary?
        for idx, feed in self.gtfs_feeds.iteritems():
            group_gtfs = pd.merge(self._group_stop_time_index,
                                  feed.stop_times,
                                  on=['file_idx','trip_id','stop_id','stop_sequence'])
            
            ofile = self.fg.next()
            gtfs_files.append(ofile)
            dump_pickle(ofile, group_gtfs)
        
        
        self._apc_group_trip_list = self._aggregate_and_apply(apc_files,
                                                              groupby=self.gtl_apc_settings.groupby, 
                                                              sortby=self.gtl_apc_settings.sortby, 
                                                              rename=self.gtl_apc_settings.rename, 
                                                              agg_args=self.gtl_apc_settings.agg_args, 
                                                              apply_args=self.gtl_apc_settings.apply_args)
        self._apc_group_trip_list.to_csv(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018May14.135146\_apc_group_trip_list.csv')
        self._apc_group_trip_list.to_hdf(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018May14.135146\_apc_group_trip_list.h5','data')
                
        self._gtfs_group_trip_list = self._aggregate_and_apply(gtfs_files,
                                                               groupby=self.gtl_gtfs_settings.groupby, 
                                                               sortby=self.gtl_gtfs_settings.sortby, 
                                                               rename=self.gtl_gtfs_settings.rename, 
                                                               agg_args=self.gtl_gtfs_settings.agg_args, 
                                                               apply_args=self.gtl_gtfs_settings.apply_args)
        self._gtfs_group_trip_list.to_csv(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018May14.135146\_gtfs_group_trip_list.csv')
        self._gtfs_group_trip_list.to_hdf(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018May14.135146\_gtfs_group_trip_list.h5','data')
        
        #trip_list = pd.DataFrame(self._trip_list, copy=True)
        group_trip_list = pd.DataFrame(self._apc_group_trip_list, copy=True)
        group_trip_list.loc[:,'scheduled_start_time'] = pd.NaT
        group_trip_list.loc[:,'scheduled_end_time'] = pd.NaT
        group_trip_list.loc[:,'scheduled_runtime'] = pd.NaT
        group_trip_list.loc[:,'scheduled_moving_time'] = pd.NaT
        group_trip_list.loc[:,'scheduled_stopped_time'] = pd.NaT
        group_trip_list = group_trip_list.reset_index().set_index(['file_idx','trip_id','direction_id'])
        
        
        gtfs_trips = self._gtfs_group_trip_list.reset_index().set_index(['file_idx','trip_id','direction_id'])
        group_trip_list.update(gtfs_trips, overwrite=False)
        
        # the update casts timedeltas to datetimes for some reason, so cast them back
        group_trip_list.reset_index(inplace=True)
        group_trip_list.loc[:,'scheduled_start_time'] = group_trip_list['scheduled_start_time'].map(lambda x: datetime_to_timedelta(x))
        group_trip_list.loc[:,'scheduled_runtime'] = group_trip_list['scheduled_runtime'].map(lambda x: datetime_to_timedelta(x))
        group_trip_list.loc[:,'scheduled_moving_time'] = group_trip_list['scheduled_moving_time'].map(lambda x: datetime_to_timedelta(x))
        group_trip_list.loc[:,'scheduled_stopped_time'] = group_trip_list['scheduled_stopped_time'].map(lambda x: datetime_to_timedelta(x))
        
        # also cast this one to timedelta
        group_trip_list.loc[:,'observed_start_time'] = group_trip_list['observed_start_time'].map(lambda x: datetime_to_timedelta(x))
        group_trip_list.loc[:,'timeperiod_id'] = apply_time_periods(group_trip_list['scheduled_start_time'], self.timeperiods)
        
        group_trip_list.rename(columns={'file_idx':'service_id'}, inplace=True)
        
        group_trip_list = group_trip_list.loc[:,['service_id','group_id','route_id','route_short_name',
                                                 'trip_id','direction_id','timeperiod_id',
                                                 'scheduled_start_time',
                                                 'scheduled_runtime',
                                                 'scheduled_moving_time',
                                                 'scheduled_stopped_time',
                                                 'observed_start_time',
                                                 'observed_runtime',
                                                 'observed_moving_time',
                                                 'observed_stopped_time',
                                                 'on',
                                                 'off',
                                                 'max_load',
                                                 'avg_load'
                                                 ]]
        self._group_trip_list = group_trip_list
        self.log.debug("done making group_trip_list")
        return
    
    def make_group_trip_list_OLD(self, weekday=True, holiday=False):
        self.log.debug("making group_trip_list")
        if not isinstance(self.groups, pd.DataFrame):
            raise Exception("make_group_stats requires groups.txt")
        
        has_thru = 'thru_node_set' in self.groups.columns
        n = []
        for idx, row in self.groups.iterrows():
            from_node_set = row['from_node_set'] if isinstance(row['from_node_set'], list) else [row['from_node_set']]
            to_node_set = row['to_node_set'] if isinstance(row['to_node_set'], list) else [row['to_node_set']]
            
            self.log.debug(from_node_set)
            self.log.debug(to_node_set)
            if has_thru:
                thru_node_set = row['thru_node_set'] if isinstance(row['thru_node_set'], list) else [row['thru_node_set']]
                self.log.debug(thru_node_set)
            
            for service_id, feed in self.gtfs_feeds.iteritems():
                nodes = feed.stop_times.loc[feed.stop_times['stop_id'].isin(from_node_set),['trip_id','stop_sequence']]
                nodes.rename(columns={'stop_sequence':'from_node_seq'}, inplace=True)
                nodes.set_index('trip_id', inplace=True)
                nodes.loc[:,'to_node_seq'] = feed.stop_times.loc[feed.stop_times['stop_id'].isin(to_node_set),].set_index('trip_id')['stop_sequence']
                self.log.debug(nodes)
                nodes.loc[:,'service_id'] = service_id
                nodes.loc[:,'group_id'] = row['group_id']
                
                if has_thru:
                    nodes.loc[:,'thru_node_seq'] = feed.stop_times.loc[feed.stop_times['stop_id'].isin(thru_node_set),].set_index('trip_id')['stop_sequence']
                    nodes.dropna(subset=['from_node_seq','to_node_seq','thru_node_seq'])
                    nodes = nodes.loc[nodes['from_node_seq'].lt(nodes['thru_node_seq']) & nodes['thru_node_seq'].lt(nodes['to_node_seq'])]
                else:
                    nodes.dropna(subset=['from_node_seq','to_node_seq'])    
                    nodes = nodes.loc[nodes['from_node_seq'].lt(nodes['to_node_seq'])]
                
                nodes = nodes.reset_index().set_index(['service_id','trip_id'])
                n.append(nodes)
                
        group_trips = pd.concat(n)
        
        if not isinstance(self._trip_list, pd.DataFrame):
            self.make_trip_list(apc_settings=self.gtl_apc_settings, gtfs_settings=self.gtl_gtfs_settings, overwrite=False)
            trip_list = pd.DataFrame(self._trip_list, copy=True)
        #trip_list = pd.DataFrame(self._trip_list, copy=True)
        trip_list.rename(columns={'file_idx':'service_id'}, inplace=True) # this should be unnecessary.
        trip_list.set_index(['service_id','trip_id'], inplace=True)
        
        cols = []
        for col in trip_list.columns:
            if col not in group_trips.columns:
                cols.append(col)
        
        group_trips = pd.merge(group_trips, trip_list.loc[:,cols], left_index=True, right_index=True)
        group_trips.reset_index(inplace=True)
        
        self._group_trip_list = group_trips
        self.log.debug("done making group_trip_list")
        return group_trips
            
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
        if not isinstance(self._trip_list, pd.DataFrame):
            self.make_trip_list()
            
        self._apc_trip_stats = self._aggregate_df(data=self._apc_trip_list,
                                                  groupby=self.ts_settings.groupby, 
                                                  sortby=self.ts_settings.sortby, 
                                                  rename=self.ts_settings.rename, 
                                                  agg_args=self.ts_settings.agg_args, 
                                                  apply_args=self.ts_settings.apply_args)
        
        trip_stats = pd.DataFrame(self._apc_trip_stats, copy=True)
        # TODO.  If this (updating gtfs route_id, route_short_name, etc.) being done in make_trip_list, it doesn't need to be done here.
        self.log.debug(trip_stats.columns)
        trip_stats.loc[:,'route_id'] = np.nan
        trip_stats.loc[:,'route_short_name'] = np.nan
        trip_stats.loc[:,'trip_id'] = np.nan
        trip_stats.loc[:,'direction_id'] = np.nan
        trip_stats.update(self.apc_to_gtfs)
        
        trip_stats.loc[:,'scheduled_start_time'] = pd.NaT
        trip_stats.loc[:,'scheduled_end_time'] = pd.NaT
        trip_stats.loc[:,'scheduled_runtime'] = pd.NaT
        trip_stats.loc[:,'scheduled_moving_time'] = pd.NaT
        trip_stats.loc[:,'scheduled_stopped_time'] = pd.NaT
        trip_stats = trip_stats.reset_index().set_index(['file_idx','trip_id','direction_id'])
        gtfs_trips = self._gtfs_trip_list.reset_index().set_index(['file_idx','trip_id','direction_id'])

        try:
            trip_stats.update(gtfs_trips, overwrite=False)
        except Exception as e:
            self.log.debug(e)
            self.log.debug('trying to convert a column')
            gtfs_trips['scheduled_start_time'] = pd.to_datetime(gtfs_trips['scheduled_start_time'])
            trip_stats.update(gtfs_trips, overwrite=False)

        # the update casts timedeltas to datetimes for some reason, so cast them back
        trip_stats.reset_index(inplace=True)
        trip_stats.loc[:,'scheduled_start_time'] = trip_stats['scheduled_start_time'].map(lambda x: datetime_to_timedelta(x))
        trip_stats.loc[:,'scheduled_runtime'] = trip_stats['scheduled_runtime'].map(lambda x: datetime_to_timedelta(x))
        trip_stats.loc[:,'scheduled_moving_time'] = trip_stats['scheduled_moving_time'].map(lambda x: datetime_to_timedelta(x))
        trip_stats.loc[:,'scheduled_stopped_time'] = trip_stats['scheduled_stopped_time'].map(lambda x: datetime_to_timedelta(x))
        self.log.debug(trip_stats.head())

        ridership = trip_stats.loc[:,['file_idx','route_id','route_short_name','trip_id','direction_id',
                                     # 'total_boardings',
                                      'avg_boardings',
                                      'stdev_boardings',
                                      'avg_alightings',
                                      'stdev_alightings',
                                      'avg_max_load',
                                      'stdev_max_load',
                                      'avg_load',
                                      'stdev_load',
                                      'samples']]
        ridership.rename(columns={'file_idx':'service_id'}, inplace=True)
        self._trip_ridership = ridership
        self.trip_ridership = df_format_datetimes_as_str(self._trip_ridership).set_index(['service_id','route_id','route_short_name','trip_id','direction_id'])

        trip_stats = trip_stats.loc[:,['file_idx','route_id','route_short_name','trip_id','direction_id',
                                       'scheduled_start_time',
                                       'scheduled_runtime',
                                       'scheduled_moving_time',
                                       'scheduled_stopped_time',
                                       'avg_observed_start_time',
                                       'stdev_observed_start_time',
                                       'semidev_observed_start_time',
                                       'avg_observed_runtime',
                                       'stdev_observed_runtime',
                                       'avg_observed_moving_time',
                                       'stdev_observed_moving_time',
                                       'avg_observed_stopped_time',
                                       'stdev_observed_stopped_time']]
        trip_stats.rename(columns={'file_idx':'service_id'}, inplace=True)
        self._trip_stats = trip_stats
        self.trip_stats = df_format_datetimes_as_str(self._trip_stats).set_index(['service_id','route_id','route_short_name','trip_id','direction_id'])
        return

    def make_route_stats(self, weekday=True, holiday=False):
        '''
        Make route_stats dataframe
        '''
        if not isinstance(self._trip_stats, pd.DataFrame):
            self.make_trip_stats()
        
        trip_stats = pd.merge(self._trip_stats, self._trip_ridership, on=['service_id','route_id','route_short_name','trip_id','direction_id'])
        trip_stats.loc[:,'timeperiod_id'] = apply_time_periods(trip_stats['scheduled_start_time'], self.timeperiods)
        #trip_stats.set_index('timeperiod_id', inplace=True)
        #self.log.debug(trip_stats.head())
        #self.log.debug(self.timeperiods)
        #self.log.debug(self.timeperiods.set_index('timeperiod_id')['timeperiod_start_time'])
        #trip_stats.loc[:,'start_time'] = self.timeperiods.set_index('timeperiod_id')['timeperiod_start_time']
        #trip_stats.loc[:,'end_time'] = self.timeperiods.set_index('timeperiod_id')['timeperiod_end_time']
        #self.log.debug(trip_stats.head())
        trip_stats.reset_index(inplace=True)
        
        route_stats = self._aggregate_df(data=trip_stats,
                                         groupby=self.rs_settings.groupby, 
                                         sortby=self.rs_settings.sortby, 
                                         rename=self.rs_settings.rename, 
                                         agg_args=self.rs_settings.agg_args, 
                                         apply_args=self.rs_settings.apply_args)
        
        #self.log.debug(self.timeperiods.set_index('timeperiod_id')['timeperiod_start_time'])
        route_stats = route_stats.reset_index().set_index('timeperiod_id')
        route_stats.loc[:,'start_time'] = self.timeperiods.set_index('timeperiod_id')['timeperiod_start_time']
        route_stats.loc[:,'end_time'] = self.timeperiods.set_index('timeperiod_id')['timeperiod_end_time']
        #trip_stats.reset_index(inplace=True)
        #self.log.debug(trip_stats.head())
        
        #route_stats.to_csv(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018May14.135146\_route_stats.csv')
        #route_stats.to_hdf(r'Q:\Model Development\SHRP2-fasttrips\Task5\sfdata_wrangler\gtfs_stat\2018May14.135146\_route_stats.h5','data')
        route_stats = route_stats.reset_index().set_index(self.rs_settings.groupby)

        route_stats.reset_index(inplace=True)
        ridership = route_stats.loc[:,['service_id','route_id','route_short_name','direction_id',
                                       'timeperiod_id','start_time','end_time',
                                     #  'total_boardings',
                                       'avg_boardings',
                                     #  'stdev_boardings',
                                       'avg_alightings',
                                     #  'stdev_alightings',
                                     #        'avg_max_load',
                                     #        'stdev_max_load',
                                     #        'avg_load',
                                     #        'stdev_load',
                                     'samples']]
        
        self._ridership_routes = ridership
        self.ridership_routes = df_format_datetimes_as_str(self._ridership_routes).set_index(['service_id','route_id','route_short_name','direction_id'])
        
        self._route_stats = route_stats.loc[:,['service_id','route_id','route_short_name','direction_id',
                                               'timeperiod_id','start_time','end_time',
                                               'avg_scheduled_headway',
                                               'avg_scheduled_runtime',
                                               'stdev_scheduled_runtime',
                                               'avg_scheduled_stopped_time',
                                               'stdev_scheduled_stopped_time',
                                               'avg_scheduled_moving_time',
                                               'stdev_scheduled_moving_time',
                                               #'avg_observed_headway',
                                               'avg_observed_runtime',
                                               #'stdev_observed_runtime',
                                               'avg_observed_stopped_time',
                                               #'stdev_observed_stopped_time',
                                               'avg_observed_moving_time',
                                               #'stdev_observed_moving_time',
                                               'samples']]
        self.route_stats = df_format_datetimes_as_str(self._route_stats).set_index(['service_id','route_id','route_short_name','direction_id','timeperiod_id'])
        return

    def make_group_stats(self, weekday=True, holiday=False):
        '''
        Make group_stats dataframe
        '''
        self.log.debug("making group_stats")
        
        if not isinstance(self._group_trip_list, pd.DataFrame):
            self.make_group_trip_list()

        group_trips = pd.DataFrame(self._group_trip_list, copy=True)
        
        group_stats = self._aggregate_df(data=group_trips,
                                         groupby=self.gs_settings.groupby, 
                                         sortby=self.gs_settings.sortby, 
                                         rename=self.gs_settings.rename, 
                                         agg_args=self.gs_settings.agg_args, 
                                         apply_args=self.gs_settings.apply_args)
        
        group_stats = group_stats.reset_index().set_index('timeperiod_id')
        group_stats.loc[:,'start_time'] = self.timeperiods.set_index('timeperiod_id')['timeperiod_start_time']
        group_stats.loc[:,'end_time'] = self.timeperiods.set_index('timeperiod_id')['timeperiod_end_time']
        group_stats.reset_index(inplace=True)
        
        self._group_stats = group_stats
        self.group_stats = df_format_datetimes_as_str(self._group_stats).reset_index().set_index(self.gs_settings.groupby)
        
        self.log.debug("done making group_stats")
        return
        
    def _aggregate_df(self, data, groupby, sortby, rename, agg_args, apply_args):
        if sortby != None:
            data.sort_values(by=sortby, inplace=True)
        self.log.debug(data.head())
        self.log.debug(data.dtypes)
        agg = data.groupby(groupby).agg(agg_args)
        
        if rename!=None:
            print "proc_aggregate.renaming columns"
            new_cols = []
            for c in agg.columns:
                try:
                    nc = rename[c]
                    new_cols.append(nc)
                except Exception as e:
                    print 'failed to rename column %s' % str(c)
                    print e
                    new_cols.append(c)
            agg.columns = new_cols
            
        if apply_args!=None:
            if not isinstance(apply_args, dict):
                raise Exception(r'apply_args must be type (dict)')
            for fname, apply_func in apply_args.iteritems():
                print "proc_apply.applying.%s" % (apply_func.__name__)
                agg[fname] = agg.apply(apply_func, axis=1)
                
        return agg
    
    # DISTRIBUTION FUNCTIONS
    def _aggregate(self, infiles, groupby, sortby, rename, agg_args):
        pass
    
    def _apply(self, infiles, apply_args):
        from dispy_processing_utils import proc_aggregate, proc_apply
        
        self.log.debug('creating iterator for apply jobs')
        apply_iter = self._iter_apply_job(infiles, apply_args)
        self.log.debug('distributing apply jobs')
        results = self._distribute(proc_apply, apply_iter)

        return results
        
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
        
        if apply_args!=None:
            self.log.debug('creating iterator for apply jobs')
            apply_iter = self._iter_apply_job(results, apply_args)
            self.log.debug('distributing apply jobs')
            results = self._distribute(proc_apply, apply_iter)

        dfs = []
        for result in results:
            df = load_pickle(result)
            dfs.append(df)

        agg = pd.concat(dfs)

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

    # distributor
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
                    self.log.debug('- %s' % job.stderr)
                    self.log.debug('- %s' % job.stdout)
                    self.log.debug('- %s' % job.exception)
                    self.log.debug('jobid %d: %s' % (jobid, result))
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
                self.log.debug('- %s' % job.stderr)
                self.log.debug('- %s' % job.stdout)
                self.log.debug('- %s' % job.exception)
                self.log.debug('jobid %d: %s' % (jobid, result))
            except Exception as e:
                self.log.warn(e)
                self.log.debug('- %s' % job.stderr)
                self.log.debug('- %s' % job.stdout)
                self.log.debug('- %s' % job.exception)
        for jobid in pop_ids:
            wait_queue.pop(jobid)
        cluster.close()
        return results