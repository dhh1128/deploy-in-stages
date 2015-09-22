#! /usr/bin/python

import os, subprocess, shlex, re, stat, sys, time, json, traceback, uuid, socket

this_script = os.path.normpath(os.path.realpath(os.path.abspath(__file__)))
my_folder = os.path.dirname(this_script)
targets_path = os.path.join(my_folder, 'targets.json')
help_pat = re.compile(r'^(/|--?)(\?|h(elp)?)$', re.I)
terminal_buckets = ['done', 'failed', 'stopped']
active_buckets = ['queued', 'pending']
all_buckets = active_buckets + terminal_buckets
remote_install_path = '~/deploy-in-stages'
dict_type = type({})
string_type = type('')
unicode_type = type(u'')


def is_string(x):
    t = type(x)
    return t == string_type or t == unicode_type


_host = None
def get_host():
    global _host
    if not _host:
        _host = socket.getfqdn()
        if _host.endswith('.es.bluecoat.com'):
            _host = _host[:-16]
    return _host
    

def complain(msg):
    sys.stderr.write('\n' + msg + '\n\n')
    sys.exit(1)
    

class json_wrapper(object):
    '''
    Make it possible to use dotted notation (x.y) in addition
    to the clumsier dict notation (x['y']) on dicts built by
    the json library.
    '''
    def __init__(self, raw):
        # This next line prevents infinite recursion that would happen if we
        # just wrote self.raw = raw. (If we did that, "self.raw" would cause
        # __getattr__ to be called, since "raw" isn't yet in self.__dict__..)
        super(json_wrapper, self).__setattr__('raw', raw)
    def __getattr__(self, name):
        # This line doesn't recurse because 'raw' is already in self.__dict__.
        raw = self.raw
        if name == 'raw':
            return raw
        if name in raw:
            answer = raw[name]
            if type(answer) == dict_type:
                return json_wrapper(answer)
            return answer
        raise KeyError
    def __repr__(self):
        return self.__dict__['raw'].__repr__()
    def __str__(self):
        return self.__dict__['raw'].__str__()
    def __setattr__(self, name, value):
        self.raw[name] = value


class job(json_wrapper):
    '''
    Manage all the state for a job, preserving important invariants. Allow
    the job to be loaded from and saved to json, and provide some utility
    methods.
    '''
    
    def __init__(self, raw):
        json_wrapper.__init__(self, raw)
    
    @staticmethod
    def empty(guid=None, sources=[], target='', queue_time=None):
        if not guid:
            guid = uuid.uuid1().hex
        if not queue_time:
            queue_time = time.time()
        raw = {
            'uuid': guid,
            'sources': sources,
            'target': target,
            'queue_time': queue_time,
            'next_try_time': 0,
            'pending': [],
            'done': [],
            'failed': [],
            'events': [],
            'stopped': [],
            'retries': {}
        }
        json_wrapper.__init__(self, raw)
        
    @staticmethod
    def load(path):
        with open(path, 'r') as f:
            return job(json.load(f))
        
    @staticmethod
    def save(path):
        with open(path, 'w') as f:
            f.write(json.dump(self.raw, indent=2))
            
    def log(self, event):
        self.events.insert(0, 'On %s at %s: %s' % (get_host(), time.asctime(), event))


def get_bucket_folder(which):
    return os.path.join(my_folder, which)
        

rsync_output_pat = re.compile(r'\s*[dwrx-]+ +([0-9,]+)')

def get_remote_size(remote_path):
    cmd = 'rsync %s' % remote_path
    try:
        stdout = subprocess.check_output(shlex.split(cmd))
        match = rsync_output_pat.match(stdout)
        if match:
            return int(match.group(1).replace(',', ''))
    except:
        pass
    return -1


def queue(sources, rsync_spec):
    try:
        if ':' not in rsync_spec:
            complain('Target should be an rsync-style host:path couplet.')
            
        host, path = rsync_spec.split(':')
        
        if not path.startswith('/') and not path.startswith('~/'):
            complain('Remote path must either be absolute or relative to ~/.')
            
        targets = get_targets()
        
        # Most commonly, the "host" will actually be a pseudo host that's
        # actually an aggregate target. However, we also want to support
        # explicit host names.
        if host in targets:
            target = targets[host]
        else:
            # Pretend host was defined
            target = { host: None }
            
        if type(sources) == type(''):
            sources = [sources]
        expanded_sources = []
        for src in sources:
            info = os.stat(src)
            this_src = {
                'path': os.path.abspath(src),
                'size': info[stat.ST_SIZE],
                'lastmod_time': info[stat.ST_MTIME]
            }
            expanded_sources.append(this_src)
            
        job = {
            'id': uuid.uuid1().hex,
            'sources': expanded_sources,
            'target': rsync_spec,
            'queue_time': time.time(),
            'next_try_time': 0,
            'pending': [],
            'done': [],
            'failed': [],
            'events': [],
            'stopped': [],
            'retries': {}
        }
        
        folder = get_bucket_folder('queued')
        if not os.path.isdir(folder):
            os.makedirs(folder)
        save_job(job, os.path.join(folder, job['id']))
        print('Job %s is queued.' % job['id'])
        
    except SystemExit:
        raise
    
    except:
        complain(traceback.format_exc())
        

def ignore_no_such_folder(errors):
    if 'No such file or directory' in errors:
        return None
    return errors

def collect_results(job, host, updated_hosts):
    # If the remote host is sending us results, make sure it's running latest script.
    if host not in updated_hosts:
        if rsync(this_script, '%s:%s/' % (host, remote_install_path)):
            updated_hosts[host] = 1

    terminal_count = 0
    for bucket in terminal_buckets:
        terminal_file = '%s:%s/%s/%s' % (host, remote_install_path, bucket, job['id'])
        local_file = '/tmp/%s' % job['id']
        if rsync(terminal_file, local_file, error_filter=ignore_no_such_folder):
            terminal_count += 1
            remote_job = load_job(local_file)
            os.remove(local_file)
            remote_evs = remote_job['events']
            local_evs = job['events']
            if remote_evs:
                for e in remote_evs:
                    local_evs.insert(0, e)
            if not bucket in job:
                job[bucket] = []
            job[bucket].append(host)
    if not terminal_count:
        log_job_event(job, 'No results yet from %s.' % host)


def move_job_to_bucket(job, old_bucket, new_bucket):
    if old_bucket == new_bucket:
        return
    
    old_path = os.path.join(my_folder, old_bucket, job['id'])
    if not os.path.isfile(old_path):
        old_path += '.busy'
        if not os.path.isfile(old_path):
            return
    new_folder = os.path.join(my_folder, new_bucket)
    if not os.path.isdir(new_folder):
        os.makedirs(new_folder)
    new_path = os.path.join(my_folder, new_bucket, job['id'])
    #print('%s --> %s' % (old_path, new_path))
    os.rename(old_path, new_path)
    log_job_event(job, 'Status is now %s.' % new_bucket)
    print('Job %s status is %s.' % (job['id'], new_bucket))
    return new_path
    

def get_next_retry(job, host):
    retry_info = job['retries'].get(host, None)
    if not retry_info:
        retry_info = {"next_time": None, "count": 0}
        job['retries'][host] = retry_info
    retry_info['count'] += 1
    if retry_info['count'] < 5:
        retry_info['next_try_time'] = time.time() + (retry_info['count']**3 * 60)
    else:
        del job['retries'][host]
        retry_info = None
    return retry_info


def distribute(job, host, remote_path, cascade_target, cascade_delay, current_bucket, updated_hosts):
    try:
        
        # If this host is going to run a dis job of its own, make sure its dis script
        # is up-to-date. This constitutes a sort of autoupdate feature; only the top of
        # a distribution hierarchy needs to have the latest code when we begin.
        if cascade_target:
            if host not in updated_hosts:
                if rsync(this_script, '%s:%s/' % (host, remote_install_path)):
                    updated_hosts[host] = 1
        
        # Now push the data.
        if rsync(job['sources'], '%s:%s' % (host, remote_path)):
            log_job_event(job, 'Successfully pushed data to %s:%s.' % (host, remote_path))
            
            # Now see if we need to define and push a job onto the remote host,
            # to cascade additional distribution work.
            if cascade_target:
                
                # Rewrite sources so they are stated in terms of the remote host's fs.
                revised_sources = [
                    {
                        'path': os.path.join(remote_path, os.path.basename(x['path'])),
                        'lastmod_time': None, # we don't know what mtime we'll have remotely
                        'size': x['size']
                    } for x in job['sources']
                ]
                
                child_job = {
                    'id': job['id'],
                    'sources': revised_sources,
                    'target': '%s:%s' % (cascade_target, remote_path),
                    'queue_time': time.time(),
                    'last_try_time': 0,
                    'next_try_time': job['queue_time'] + (cascade_delay * 60),
                    'pending': [],
                    'done': [],
                    'failed': [],
                    'events': [],
                    'stopped': [],
                    'retries': {}
                }
                log_job_event(child_job, 'Delegated.')
                
                # We put the job in a 'queued' subdir because if the queued
                # subdir doesn't yet exist remotely, we'll end up putting
                # a raw file in the wrong place, unless we replicate a whole
                # folder...
                repl = '/tmp/queued'
                if not os.path.isdir(repl):
                    os.makedirs(repl)
                else:
                    items = [os.path.join(repl, x) for x in os.listdir(repl)]
                    for x in items:
                        os.remove(x)
                fname = '%s/%s' % (repl, job['id'])
                save_job(child_job, fname)
                
                # Send the job to the remote machine.
                succeeded = True
                if rsync(repl, '%s:%s/' % (host, remote_install_path)):
                    log_job_event(job, 'Queued remote job on %s.' % host)
                    job['pending'].append(host)
                else:
                    log_job_event(job, 'Unable to queue remote job on %s.' % host)
                    retry_info = get_next_retry(job, host)
                    if not retry_info:
                        log_job_event(job, 'Attempt to queue job on %s definitively failed and will not be retried.' % host)
                        job['failed'].append(host)
                    else:
                        log_job_event(job, 'Attempt to queue job on %s failed, but retry is scheduled.' % host)
                    succeeded = False
                os.remove(fname)
                return succeeded
                
            else:
                log_job_event(job, 'No delegation necessary; %s is a terminal distribution point.' % host)
                job['done'].append(host)
                return True
            
        else:
            log_job_event(job, 'Unable to push data to %s.' % host)
            retry_info = get_next_retry(job, host)
            if not retry_info:
                log_job_event(job, 'Attempt to push data to %s definitively failed and will not be retried.' % host)
                job['failed'].append(host)
            else:
                log_job_event(job, 'Attempt to push data to %s failed, but retry is scheduled.' % host)
            return False
        
    except Exception as e:
        traceback.print_exc()
        log_job_event(job, str(e))
        job['failed'].append(host)
        return False
    
def rsync(src, tgt=None, error_filter=None):
    # Src can be a string or a list of objects that contain path+lastmod+size
    if type(src) != type([]):
        src = [src]
        
    src_is_local = None
    
    for src_item in src:
        recurse_switch = ''
        
        is_obj = type(src_item) == type({})
        if src_is_local is None:
            src_is_local = is_obj or (':' not in src_item)
            
        src_path = src_item
        if src_is_local:
            if is_obj:
                bad = False
                src_path = src_item['path']
                info = None
                try:
                    info = os.stat(src_path)
                    # We may or may not have a lastmod, depending on whether the job
                    # is newly written by a remote node. If we do, compare it to the
                    # modtime we have now.
                    old_lastmod = src_item['lastmod_time']
                    if old_lastmod:
                        if info[stat.ST_MTIME] != old_lastmod:
                            bad = true
                    if info[stat.ST_SIZE] != src_item['size']:
                        bad = True
                    #print("ready to make the attempt")
                except:
                    traceback.print_exc()
                    bad = True
                if bad:
                    if info:
                        raise Exception('%s has changed since it was queued; new size = %s, new lastmod_time = %s.' %
                                        (src_path, info[stat.ST_SIZE], info[stat.ST_MTIME]))
                    else:
                        raise Exception('%s has been deleted since it was queued.' % src_path)
                
        if os.path.isdir(src_path):
            recurse_switch = '-r '
        cmd = 'rsync %s%s %s' % (recurse_switch, src_path, tgt)
        print(cmd)
        errors = None
        try:
            proc = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, stderr = proc.communicate()
            if proc.returncode:
                errors = ('%s\n%s\n%s' % (cmd, stdout, stderr)).strip().replace('\n\n', '\n')
        except subprocess.CalledProcessError as e:
            errors = '%s\n%s' % (cmd, e.output).strip()
        if errors:
            if error_filter:
                errors = error_filter(errors)
            if errors:
                print(errors)
            return False
    return True


def parse_cascade_spec(spec, host):
    cascade_delay = 0
    if spec:
        if ' ' in spec:
            cascade_target, cascade_delay = spec.split(' ')
            cascade_delay = int(cascade_delay)
        else:
            cascade_target = cascade_spec
    else:
        cascade_target = None
        cascade_delay = None
    return cascade_target, cascade_delay


def handle_bucket(which, targets, tallies, updated_hosts):
    folder = get_bucket_folder(which)
    
    #print('checking %s' % folder)
    for item in bucket_items(which):
        
        try:
            job_path = os.path.join(folder, item)
            
            # Make sure we can load the .json for the job
            try:
                job = load_job(job_path)
            except:
                tallies.skipped += 1
                traceback.print_exc()
                print('Ignored %s/%s; not a valid json job.' % (which, item))
                continue

            # Is this job mature (ready to be checked)?            
            now = time.time()
            if job['next_try_time'] <= now:
                
                # Just in case, temporarily change the filename to show that
                # we're working with the job. This will prevent multiple copies
                # of the script that are running in parallel from stepping on
                # one another's toes.
                try:
                    #print("marking %s as busy" % job_path)
                    job_path = mark_busy(job_path)
                except:
                    # If we failed to rename, log issue and continue.
                    print('Unable to mark %s busy. Is another copy of this script running?' % job_path)
                    continue
                
                try:
                    #print("splitting spec")
                    target, remote_path = job['target'].split(':')
                    target = targets.get(target, {target: None})
                    hosts = target.keys()
                    done = job['done']
                    failed = job['failed']
                    pending = job['pending']
                    some_were_pending = bool(pending)
                    collected_count = 0
                    
                    for host in hosts:
                        #print("handling host %s" % host)
                        if host in pending:
                            if collect_results(job, host, updated_hosts):
                                tallies.delegates_changed += 1
                        elif host in done or host in failed:
                            # We don't need to react to stuff we already got to a
                            # terminal status.
                            pass
                        else:
                            #print('checking retries')
                            retry_info = job['retries'].get(host, None)
                            if not retry_info or (retry_info['next_try_time'] < now):
                                #print('attempting to distribute')
                                cascade_target, cascade_delay = parse_cascade_spec(target[host], host)
                                if distribute(job, host, remote_path, cascade_target, cascade_delay, which, updated_hosts):
                                    tallies.distributed += 1
                                else:
                                    tallies.retries += 1
                                
                    # Did we reach a terminal status on overall job?
                    if len(done) + len(failed) == len(hosts):
                        if failed:
                            tallies.failed += 1
                            new_bucket = 'failed'
                        else:
                            tallies.done += 1
                            new_bucket = 'done'
                        job_path = move_job_to_bucket(job, which, new_bucket)
                    else:
                        if not some_were_pending:
                            if pending:
                                tallies.pending += 1
                                job_path = move_job_to_bucket(job, which, 'pending')
                        job['next_try_time'] = now + 60
                        
                finally:
                    # Rewrite job file to capture events that happened.
                    save_job(job, job_path)
                    # Guarantee that the job is not marked busy when we leave.
                    mark_busy(job_path, False)
            else:
                tallies.idle += 1
        except:
            traceback.print_exc()
            continue
        
class tallies:
    def __init__(self):
        self.distributed = 0
        self.retries = 0
        self.delegates_changed = 0
        self.done = 0
        self.failed = 0
        self.pending = 0
        self.idle = 0
        self.skipped = 0
    def is_empty(self):
        return self.distributed + self.retries + self.delegates_changed + self.done + self.failed + self.pending + self.idle + self.skipped == 0
    def __str__(self):
        return '''In this pass:
  %d payloads were transferred
  %d retries were scheduled
  %d delegated jobs changed status
  %d jobs finished successfully
  %d jobs failed
  %d jobs were delegated to a remote node
  %d jobs were idle awaiting maturity
  %d jobs were skipped because they were malformed
''' % (self.distributed, self.retries, self.delegates_changed, self.done, self.failed, self.pending, self.idle, self.skipped)


def more(quiet=False):
    print('\nChecking for more dis work on %s.' % time.asctime())
    targets = get_targets()
    tally = tallies()
    updated_hosts = {}
    handle_bucket('queued', targets, tally, updated_hosts)
    handle_bucket('pending', targets, tally, updated_hosts)
    if quiet and tally.is_empty():
        print('  ...nothing to do.')
        return
    print(tally)
    
        
def mark_busy(path, busy=True):
    if path.endswith('.busy'):
        busy_path = path
        path = path[:-5]
    else:
        busy_path = path + '.busy'
    if busy:
        #print('%s --> %s' % (path, busy_path))
        os.rename(path, busy_path)
        return busy_path
    else:
        if os.path.isfile(busy_path):
            #print('%s --> %s' % (busy_path, path))
            os.rename(busy_path, path)
        return path
    

def bucket_items(which):
    folder = get_bucket_folder(which)
    if os.path.isdir(folder):
        items = [x for x in os.listdir(folder) if not x.endswith('.busy')]
        return items
    return []
    

def get_next_retry_time(job):
    next_retry = None
    pending = job['pending']
    done = job['done']
    failed = job['failed']
    target, remote_path = job['target'].split(':')
    target = get_targets().get(target, {target: None})
    hosts = target.keys()
    for host in hosts:
        if host not in pending and host not in done and host not in failed:
            retry_info = job['retries'].get(host, None)
            if retry_info:
                nt = retry_info['next_try_time']
                if next_retry is None or nt < next_retry:
                    next_retry = nt
    return next_retry


def status(buckets=None):
    if not buckets:
        buckets = active_buckets
    else:
        buckets = [buckets]
    for bucket in buckets:
        for item in bucket_items(bucket):
            try:
                job = load_job(os.path.join(get_bucket_folder(bucket), item))
                print('\n%s: %s' % (job['id'], bucket))
                srcs = job['sources']
                src = srcs[0]
                try:
                    src = src['path']
                except:
                    src = str(src)
                if len(srcs) > 1:
                    src += ' + %d more' % len(srcs) - 1
                print('  %s --> %s' % (src, job['target']))
                print('  done   : ' % job['done'])
                print('  failed :' % job['failed'])
                print('  stopped:' % job['stopped'])
                if bucket in active_buckets:
                    print('  pending: ' % job['pending'])
                    next_time = job['next_try_time']
                    next_retry = get_next_retry_time(job)
                    if next_retry and (next_retry > next_time):
                        next_time = next_retry
                        next_retry = ' (RETRY)'
                    else:
                        next_retry = ''
                    if next_time < time.time():
                        next_retry += ' -- now!'
                    last_timestamp = time.asctime(time.localtime(job.get('last_try_time', 0)))
                    next_timestamp = time.asctime(time.localtime(next_time))
                    print('  last check time: %s' % last_timestamp)
                    print('  next eligible time: %s%s' % (next_timestamp, next_retry))
                print('  log:')
                evs = job['events']
                if evs:
                    i = 0
                    while i < 5:
                        print('    %s' % evs[i])
                        i += 1
                        if i == len(evs):
                            break
            except:
                traceback.print_exc()
                print('%s/%s is unparseable.' % (bucket, item))
            print('')


def find_job(id):
    id = id.lower()
    for bucket in all_buckets:
        items = bucket_items(bucket)
        for item in items:
            if item.lower().startswith(id):
                return item, bucket
    return None, None

def stop(id_fragment, second_try=False):
    id, bucket = find_job(id_fragment)
    if id:
        if bucket not in active_buckets:
            print('Job %s is already %s.' % (id, bucket))
        else:
            job = load_job(os.path.join(get_bucket_folder(bucket), id))
            move_job_to_bucket(job, bucket, 'stopped')
        return True
    
    # In case we failed to find an id because the item was busy...
    if not second_try:
        time.sleep(5)
        if not stop(id_fragment, True):
            complain("Unable to find job %s..." % id_fragment)


def reset(id_fragment):
    id, bucket = find_job(id_fragment)
    if not id:
        complain("Unable to find job %s..." % id_fragment)
    if bucket == 'done':
        print('Job %s is already done.' % id)
        
    job_path = os.path.join(get_bucket_folder(bucket), id)
    job = load_job(job_path)
    job['retries'] = {}
    job['done'] = job['failed'] = job['pending'] = []
    job['next_try_time'] = 0
    log_job_event(job, 'Reset.')
    save_job(job, job_path)
    move_job_to_bucket(job, bucket, 'queued')
    if bucket == 'queued':
        print('Job %s has been reset.' % id)
        
        
def age(id_fragment):
    id, bucket = find_job(id_fragment)
    if not id:
        complain("Unable to find job %s..." % id_fragment)
    if bucket not in active_buckets:
        complain('Job %s is already %s.' % (id, bucket))
        
    job_path = os.path.join(get_bucket_folder(bucket), id)
    job = load_job(job_path)
    job['next_try_time'] = 0
    retries = job['retries']
    if retries:
        for key in retries.keys():
            retries[key]['next_try_time'] = 0
    log_job_event(job, 'Aged job so it is ready to attempt again.')
    save_job(job, job_path)
    print('Job %s has been aged so it is ready to attempt again.' % id)


def make_target_list():
    x = get_targets().keys()
    if not x:
        x = "undefined"
    if len(x) == 1:
        x = '"%s"' % x[0]
    elif (len(x)) == 2:
        x = '"%s" and "%s"' % (x[0], x[1])
    else:
        x = '"' + '", "'.join(get_targets().keys()) + '"'
        last_comma = x.rfind(',')
        x = x[:last_comma + 1] + ' and' + x[last_comma + 2:]
    return x


def help(args=['--h']):
    txt = '''
dis -- deploy in stages

Use rsync to push data out to a deployment hierarchy, with delays between
each stage in the hierarchy to allow us to detect and react to possible
problems.

dis queue <src> [<src>...] <target:path>

    Submit a new deploy job.
    
    - src: one or more local files/folder to deploy via rsync. This arg is
          repeatable so shell globbing is usable.
    - target:path: an rsync-style host:path pair, except that the host part
          is a deployment target defined in targets.json; currently, valid
          values are %s.
    
dis more [--quiet]

    Do any pending work to advance deploy jobs to the next stage, if their
    waiting period has elapsed. Do nothing if queue is empty. Suitable for
    scheduling as a command in cron.
    
dis status [<folder>]

    Show status on jobs. By default, all jobs that are not at a terminal
    status are displayed; if a folder such as "failed" or "done" is specified,
    the report scope is narrowed accordingly.
    
dis reset <job_id>

    Clear an incomplete job's state so it's ready to run again. Job id can be
    any convenient prefix, instead of the full value.    
    
dis age <job_id>

    Expire any local timeouts on a job, so it will progress immediately with
    the next "more" command.
    
dis stop <job_id>

    Interrupt a job. No rollback is performed, but any future work is
    canceled. Job id can be any convenient prefix, instead of the full
    value.

dis update <target>

    Push latest version of this script to specified target.
    
''' % make_target_list()
    if (len(args) != 1 or not help_pat.match(args[0])):
        complain('Error in cmdline syntax.\n\n' + txt)
    print(txt)
    sys.exit(0)
    

if __name__ == 'x__main__':
    args = sys.argv[1:]
    if args:
        verb = args[0]
        args = args[1:]
    if globals().has_key(verb):
        func = globals()[verb]
        func(*args)
    else:
        help(args)
