import sys, os, threading
import dispy
import cPickle as pickle

class config():
    def __init__(self, filename=None, nodes=None):
        self.lower_bound = 20
        self.upper_bound = 22
        self.nodes = ['172.30.1.117'] if nodes==None else nodes
        if not isinstance(self.nodes, list):
            self.nodes = [self.nodes]

        if filename:
            execfile(filename)
            
def filename_generator(basename='data_file_', ext='.dat', start=1):
    '''
    An iterator to generate filenames with format 
    <basename><zero-padded integer><ext>
    '''
    i = start
    while True:
        yield r'%s%08d%s' % (basename, i, ext)
        i = i + 1
        
def job_callback(job): # executed at the client
    global submit_queue, jobs_cond, lower_bound
    if (job.status == dispy.DispyJob.Finished  # most usual case
        or job.status in (dispy.DispyJob.Terminated, dispy.DispyJob.Cancelled,
                          dispy.DispyJob.Abandoned)):
        # 'pending_jobs' is shared between two threads, so access it with
        # 'jobs_cond' (see below)
        jobs_cond.acquire()
        if job.id: # job may have finished before 'main' assigned id
            submit_queue.pop(job.id)
            dispy.logger.info('job "%s" done with %s: %s', job.id, job.result, len(submit_queue))
            if len(submit_queue) <= lower_bound:
                jobs_cond.notify()
        else:
            dispy.logger.info('no job.id')
        jobs_cond.release()
        
def load_pickle(filename):
    f = file(filename, 'rb')
    data = pickle.load(f)
    f.close()
    return data

def dump_pickle(filename, data):
    f = file(filename,'wb')
    pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)
    f.close()
    return

def proc_stop_time_stats(infile, outfile, groupby, stat_args):
    import datetime as dt
    import pandas as pd
    import cPickle as pickle
    #from gtfs_stat import meantime, stdtime
    
    print "proc_stop_time_stats.loading"
    apc = load_pickle(infile)
    print "proc_stop_time_stats.aggregating"
    stop_time_stats = apc.groupby(groupby).agg(stat_args)
    print "proc_stop_time_stats.dumping"
    dump_pickle(outfile, stop_time_stats)
    print "proc_stop_time_stats.returning"
    return outfile

def proc_combine_stop_time_stats():
    import pandas as pd
    import cPickle as pd
    return
    
def print_dispy_job_error(job):
    print '-------------------------------'
    print '- jobid: %5d                 -' % job.id
    print '- %s' % job.status
    print '- %s' % job.stderr
    print '- %s' % job.stdout
    print '- %s' % job.exception
    print '-------------------------------'