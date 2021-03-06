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

def proc_aggregate(infile, outfile, groupby, sortby, rename, agg_args):
    import datetime as dt
    import pandas as pd
    import cPickle as pickle
    
    print "proc_aggregate.loading"
    data = load_pickle(infile)
    print "proc_aggregate.aggregating"
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
                print 'failed to rename column %s' % str(c)
                print e
        agg.columns = new_cols
            
    print "proc_aggregate.dumping"
    dump_pickle(outfile, agg)
    print "proc_aggregate.returning"
    return outfile

def proc_apply(infile, outfile, apply_args, axis):
    import datetime as dt
    import pandas as pd
    import cPickle as pickle
    
    print "proc_apply.loading"
    data = load_pickle(infile)
    print "proc_apply.applying"
    if not isinstance(apply_args, dict) and not isinstance(apply_args, list):
        raise Exception(r'apply_args must be type (dict)')
    if isinstance(apply_args, dict):
        apply_args = [apply_args]
    for app_args in apply_args:
        for fname, apply_func in app_args.iteritems():
            if isinstance(apply_func, list):
                print "proc_apply.applying.%s" % (apply_func[0].__name__)
                data[fname] = data.apply(apply_func[0], axis=axis, **apply_func[1])
            else:
                print "proc_apply.applying.%s" % (apply_func.__name__)
                try:
                    data[fname] = data[fname].apply(apply_func)
                except:
                    data[fname] = data.apply(apply_func, axis=axis)
            
    print "proc_apply.dumping"
    dump_pickle(outfile, data)
    return outfile

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