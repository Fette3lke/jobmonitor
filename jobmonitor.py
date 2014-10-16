import sys
import os
import mylib
import mysqlite
import time
import math
import re
import logging
import shutil
import numpy as np
from subprocess import *

# non-user flags
PROCESSING = 1 << 30
ERROR = 1 << 31
BUSY = (PROCESSING | ERROR)

class myProcess(Popen):
    """
    class to keep track of subprocess information
    """
    def __init__(self, *args, **kwargs):
        self.nodelist = kwargs.pop("nodelist", None)
        self.info      = kwargs.pop("info", None)
        self.status   = kwargs.pop("status", 0)
        self.outfile  = kwargs.pop("outfile", None)
        self.cwd      = kwargs.get("cwd", None)
        super(myProcess, self).__init__(*args, **kwargs)

    def isRunning(self):
        if self.poll() is None:
            self.status |= 1
            return True
        self.status &= ~1
        return False

    def isAborting(self):
        if self.status & 2:
            return True
        return False

class jobmonitor(object):
    """
    spawns jobs (remotely) and keeps track of their status
    database is updated accordingly
    manages available ressources
    """
    def __init__(self, args=None, defaults=None, cfg="./jobmonitor.cfg"):
    # possible command line arguments [shortcut, name, default value (False for simple 0/1 flag), description]
        self.args = [
            ["db", "database", True, "<sqlite3 database file name>"],
            ["cfg", "configfile", cfg, "<configuration file to load>"],
            ["p", "prefix", True, "<simulation prefix>"],
            ["t", "polltime", True, "<polltime to check running processes in seconds>"],
            ["w", "walltime",True, "<Wallclock time>"],
            ["l", "logfile", True],
            ["lvl", "loglvl", True],
            ["h", "hostfile", True,"<file containing hostnames>"]
            ]
        if not args == None:
            self.args.extend(args)
        cla = mylib.readCommandLine(self.args)
        if not cla["rcla_status"]:
            print >> sys.stderr, cla["error"]
            mylib.usage(self.args)
            sys.exit()

        # possible user-defined event handlers
        self.onSuccess = None
        self.onFail    = None
        self.onSubmit  = None
        self.onAbort   = None

        config = mylib.readConfigFile(os.getenv("HOME") + "/.jobmonitor.cfg")
        config.update(mylib.readConfigFile(cla["configfile"]))

        config.update(cla)

        for key in config:
            if config[key] and isinstance(config[key], str) and re.search("[^0-9]", config[key]) is None:
                config[key] = int(config[key])

        # default values for obligatory parameters, if set to None the parameter has to be set in the configfile or command line arguments
        self.defaults = [["username", os.getenv("USER")],
                    #            ["scriptdir", None],
                         ["scriptname", None],
                         ["abortscript", False],
                         ["database", None],
                    #            ["parampath", None],
                         ["outputpath", None],
                         ["subdir", ""],
                         ["prefix", ""],
                         ["walltime", None],
                         ["runtime", 900],
                         ["polltime", 60],
                         ["table", None],
                         ["namecolumn", "ID"],
                         ["nodecolumn", "nodes"],
                         ["flag_marked", 1],
                         ["flag_success", 2],
                         ["logfile", "./log.txt"],
                         ["loglvl", "WARNING"],
                         ["hostfile", None],
                         ["scheduler", "SGE"],
                         ["remote", False]
#                    ["ic_resolution", None]
                    #            ["",None]
                    ]
        if not defaults == None:
            self.defaults.extend(defaults)
        for [key, value] in self.defaults:
            if not key in config:
                config[key] = value
            if config[key] == None:
                print "Config error: missing value for: %s" % key
                sys.exit()

        config["startup"] = time.time()
        logging.basicConfig(format='%(asctime)s|%(levelname)s\t%(message)s', datefmt='%m/%d/%Y %H:%M:%S ',filename=config['logfile'], filemode='w')
        self.logger = logging.getLogger('jobmonitor')
        self.logger.setLevel(eval("logging."+config['loglvl']))

        self.logger.debug('started')

        # read the database
        db = mysqlite.db_init(config["database"])
        if not db:
            print >> sys.stderr, "database %s cannot be opened" % config["database"]
            sys.exit()
        self.dbcur = db.cursor()

        info = db.execute("SELECT * FROM info").fetchone()
        if not info[1] == 'simulation_database':
            print "invalid simulation database: %s" % config["database"]
            sys.exit()
        
        self.config = config
        self.db = db

        # read nodefile and store in array to keep track which ones are in use
        nodes = []
        with open(config['hostfile'], 'r') as f:
            for line in f:
                nodes.append((line.rstrip(), False))
        self.nodes = np.array(nodes, dtype = np.dtype([('name', np.str_, 256), ('used', np.bool_)]))
        if self.config['remote'] and len(self.nodes) > 1:
            self.nodes[0]['used'] = True

    def test(self):
        for key in self.config:
            os.environ['JM_'+key.upper()] = str(self.config[key])
        outf = open('test', 'a')
        print self.config['scriptname']
        Popen([self.config['scriptname'], str('testing123')], stdout=outf, stderr=STDOUT)
        outf.close()
        
    def callEvent(self, func, msg=None, info=None):
        """
        call Event handlers and call sql query if needed
        """
        if func:
            query = self.func(msg, self.config, info)
            if query:
                self.dbcur.execute(query['sql'], query['args'])


    def start(self):
        """
        alias for jobmonitor.loop()
        """
        return self.loop()

    def loop(self):
        """
        enter main monitoring loop, jobs will be submitted here
        """
        Procs = []
        nodesUsed = 0
        unused, = np.where(self.nodes['used'] == False)
        nnodes = len(self.nodes[unused])
        limit = nnodes
        if limit < 1:
            limit = 1
        
        for key in self.config:
            os.environ['JM_'+key.upper()] = str(self.config[key])

        node = 0
############################################################################
# main loop, creating and monitoring jobs
############################################################################
        while 1:
            update = []

############################################################################
# poll jobs, check whether they have finished, update database accordingly #
############################################################################
            for process in Procs[:]:
                if not process.isRunning():
                    status = process.info['status']
                    if process.isAborting():
                        status = (process.info['status']) & ~PROCESSING
                        self.logger.info("process %s aborted", self.config['prefix']+str(process.info[self.config['namecolumn']]) )
                        self.callEvent(self.onAbort, info=process.info)
                    else:
#                        outdir = "%s/%s%d/%s" % (self.config['outputpath'], self.config['prefix'], process.info[self.config['namecolumn']], self.config['subdir'])
                        outdir = process.cwd
                        tmpdir = "%s/tmp" % (outdir)
#                        outfile = "%s/%s%d%s" % (tmpdir, self.config['prefix'], process.info[self.config['namecolumn']], ".out")
                        outfile = process.outfile
                        outf = open(outfile, 'r')
                        output = outf.readlines()
                        outf.close()                    
                        # scripts should finish with string 'SUCCESS' in $JM_OUTFILE
                        proc_status = "FAIL"
                        if re.search("SUCCESS", output[-1]) is not None:
                            # set flag for success, remove flag for marked and processing
                            status = (process.info['status'] | self.config['flag_success']) & ~self.config['flag_marked'] & ~PROCESSING
                            proc_status = "SUCCESS"
                            self.callEvent(self.onSuccess, msg=output[-1], info=process.info)
                        else:
                            outf = open(outfile, 'a')
                            out = process.communicate()[0]
                            if out:
                                outf.write(out)
                            outf.close()
                            status = (process.info['status'] | ERROR) & ~self.config['flag_marked'] & ~PROCESSING
                            self.callEvent(self.onFail, msg=output[-1], info=process.info)

                        self.logger.info("process %s finished with status %s | nodesUsed %d", 
                                         self.config['prefix']+str(process.info[self.config['namecolumn']]), 
                                         proc_status, nodesUsed)
                        
                    update.append((status, process.info['ID']))
                    nodesUsed -= len(process.nodelist)
                    for node in process.nodelist:
                        self.nodes[node]['used'] = False
                    Procs.remove(process)

############################################################################
# query database for jobs to start
# no jobs will be started if the leftover walltime is less than runtime
# (processes will be aborted instead if abortscript is specified)
############################################################################
            if ((time.time() - self.config['startup']) < (self.config['walltime'] - self.config['runtime'])):
                if self.config.has_key('order'):
                    order = "ORDER BY %s" % self.config['order']
                else:
                    order = ""
                query = self.dbcur.execute("SELECT * FROM {0} WHERE status=:marked AND NOT status & :busy LIMIT :limit {1}"
                                           .format(self.config['table'], order), 
                                           {"limit": limit, "marked": self.config['flag_marked'], 
                                            "busy": BUSY})

                # start new jobs if enough unused nodes are available
                for row in query:
                    unused, = np.where(self.nodes['used'] == False)
                    if len(unused) == 0:
                        break
                    nodename = self.nodes[ unused[0] ]['name']
                    if self.config['nodecolumn'] in  row.keys():
                        if nodesUsed + row[self.config['nodecolumn']] > nnodes:
                            continue
                        nodelist = unused[:row[self.config['nodecolumn']]]
                        if len(nodelist) < row[self.config['nodecolumn']]:
                            break
                    else:
                        if nodesUsed >= nnodes:
                            break                        
                        nodelist = np.array([unused[0]])                    

                    # create directories, copy files
                    outdir = "%s/%s%d/%s" % (self.config['outputpath'], self.config['prefix'], row[self.config['namecolumn']], self.config['subdir'])
                    tmpdir = "%s/tmp" % (outdir)
                    outfile = "%s/%s%d%s" % (tmpdir, self.config['prefix'], row[self.config['namecolumn']], ".out")
                    mylib.mkdirs(tmpdir)
                    scriptname = os.path.basename(self.config['scriptname'])
                    scriptname = os.path.join(outdir, scriptname)
                    shutil.copyfile(self.config['scriptname'], scriptname)
                    shutil.copymode(self.config['scriptname'], scriptname)
                    if self.config['abortscript']:
                        abortscriptname = os.path.basename(self.config['abortscript'])
                        abortscriptname = os.path.join(outdir, abortscriptname)
                        shutil.copyfile(self.config['abortscript'], abortscriptname)
                        shutil.copymode(self.config['abortscript'], abortscriptname)                        

                    # create hostfile for this job
                    hostfile = os.path.join(outdir, 'hostfile')
                    with open(hostfile, 'w') as hf:
                        for node in nodelist:
                            hf.write('%s\n' % self.nodes[node]['name'])

                    # create file to pass variables to job-script
                    # job-script should source 'exports.sh'
                    exportfile = os.path.join(outdir, 'exports.sh')
                    with open(exportfile, 'w') as ef:
                        ef.write('#!/bin/bash\n')
                        for key in self.config:
                            ef.write('export %s=%s\n' % ('JM_'+key.upper(), self.config[key]))
                        ef.write('export %s=%s\n' % ('JM_OUTFILE', outfile) )
                        ef.write('export %s=%s\n' % ('JM_NODES', hostfile) )
                        ef.write('#%s | %s\n' % (nodename, scriptname))
                    if self.config['remote']:
                        if self.config['remote'] == "mpirun":
                            cmd = ["mpirun", "-np", "1", "--host", nodename, scriptname, str(row[self.config['namecolumn']])]
                        elif self.config['remote'] == "aprun":
                            cmd = ["aprun", "-n", "1", "-N", "1", "-m", "32G", "-L", nodename, scriptname, str(row[self.config['namecolumn']])]
                        else:
                            print "unknown remote command! supported are 'mpirun' and 'aprun'"
                            sys.exit()
                    else:
                        cmd = [scriptname, str(row[self.config['namecolumn']])]

                    # invoke script (on remote node if config['remote'] is set) and store process in dictionary, mark nodes as in use
                    # if remote is not set (or False) the job-script should use the generated hostfile ($JM_NODES)
                    # and call the parallel environment itself

#                    Procs.append((Popen(cmd, cwd=outdir, stdout=PIPE, stderr=STDOUT, shell=False), row, nodelist))
                    Procs.append( myProcess(cmd, cwd=outdir, stderr=STDOUT, shell=False, info=row, nodelist=nodelist, status=1, outfile=outfile) )
                    self.callEvent(self.onSubmit, info=row)
#                    Procs.append((Popen(cmd, cwd=outdir, stderr=STDOUT, shell=False), row, nodelist))
                    for node in nodelist:
                        self.nodes[node]['used'] = True
                    status = row['status'] | PROCESSING
                    update.append((status, row['ID']))
                    nodesUsed += len(nodelist)
                    self.logger.info("process started %s", self.config['prefix']+str(row[self.config['namecolumn']]))

            # leftover walltime is less than runtime -> abort processes
            elif self.config['abortscript']:
                for process in Procs:
                    process.status |= 2
                    abortscriptname = os.path.basename(self.config['abortscript'])
                    abortscriptname = os.path.join(process.cwd, abortscriptname)
                    Popen([abortscriptname, str(row[self.config['namecolumn']])], cwd=process.cwd)
                    self.logger.info("sent abort to process %s", self.config['prefix']+str(row[self.config['namecolumn']]))

            # update database if status of a job changed
            if update:
                self.dbcur.executemany("UPDATE {0} SET STATUS=? WHERE ID=?".format(self.config['table']), update)
                self.db.commit()
                update = []

            # stop if no processes are left in the monitor list
            if len(Procs) > 0:
                self.logger.debug("processes: %d | nodes used %d", len(Procs), nodesUsed)
                time.sleep(self.config['polltime'])
            else:
                break

        # mark all halos that are still processing as erroneous (commented out for now, will think of better detection of failed processes)
        self.logger.debug('main loop finished')
#        self.dbcur.execute("UPDATE {0} SET status = status | ? WHERE status & ? and status & ?".format(self.config['table']), (ERROR, self.config['flag_marked'], PROCESSING))
        self.db.commit
        return 0
