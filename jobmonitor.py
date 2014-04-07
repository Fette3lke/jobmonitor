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

class jobmonitor():
    def __init__(self, args=None, defaults=None, cfg="./jobmonitor.cfg"):
    # possible command line arguments [shortcut, name, default value (False for simple 0/1 flag), description]
        self.args = [
            ["db", "database", True, "<sqlite3 database file name>"],
            ["cfg", "configfile", cfg, "<configuration file to load>"],
            ["p", "prefix", True, "<simulation prefix>"],
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

        self.onSuccess = None

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
                         ["database", None],
                    #            ["parampath", None],
                         ["outputpath", None],
                         ["prefix", ""],
                         ["walltime", None],
                         ["runtime", 900],
                         ["polltime", 60],
                         ["table", None],
                         ["namecolumn", "ID"],
                         ["flag_marked", 1],
                         ["flag_success", 2],
                         ["logfile", "./log.txt"],
                         ["loglvl", "WARNING"],
                         ["hostfile", None],
                         ["scheduler", "SGE"],
                         ["remote", True]
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
        self.logger = logging.getLogger('create-ics')
        self.logger.setLevel(eval("logging."+config['loglvl']))

        self.logger.debug('started')

        # read the simulation-database
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

        nodes = []
        with open(config['hostfile'], 'r') as f:
            for line in f:
                nodes.append((line.rstrip(), False))
        self.nodes = np.array(nodes, dtype = np.dtype([('name', np.str_, 256), ('used', np.bool_)]))
        if not self.config['remote'] and len(self.nodes) > 1:
            self.nodes[0]['used'] = True

    def test(self):
        for key in self.config:
            os.environ['JM_'+key.upper()] = str(self.config[key])
        outf = open('test', 'a')
        print self.config['scriptname']
        Popen([self.config['scriptname'], str('testing123')], stdout=outf, stderr=STDOUT)
        outf.close()

    def loop(self):
        # Select halos that are marked and start to create ICs for them -> flag PROCESSING
        Procs = []
        procsUsedRefine = 0
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
            for proc, row, nodelist in Procs[:]:
                if proc.poll() is not None:
                    outdir = "%s/%s%d" % (self.config['outputpath'], self.config['prefix'], row[self.config['namecolumn']])
                    tmpdir = "%s/tmp" % (outdir)
                    outfile = "%s/%s%d%s" % (tmpdir, self.config['prefix'], row[self.config['namecolumn']], ".out")
                    outf = open(outfile, 'r')
                    output = outf.readlines()
                    outf.close()                    
                    # scripts should finish with string 'SUCCESS' in $JM_OUTFILE
                    proc_status = "FAIL"
                    if re.search("SUCCESS", output[-1]) is not None:
                        status = (row['status'] | self.config['flag_success']) & ~self.config['flag_marked'] & ~PROCESSING
                        update.append((status, row['ID']))
                        proc_status = "SUCCESS"
                        if self.onSuccess:
                            query = self.onSuccess(output[-1], self.config, row)
                            if query:
                                self.dbcur.execute(query['sql'], query['args'])
                    else:
                        outf = open(outfile, 'a')
                        out = proc.communicate()[0]
                        outf.write(out)
                        outf.close()
                        status = (row['status'] | ERROR) & ~self.config['flag_marked'] & ~PROCESSING
                        update.append((status, row['ID']))


                    nodesUsed -= len(nodelist)
                    for node in nodelist:
                        self.nodes[node]['used'] = False
                    Procs.remove((proc, row, nodelist))
                    self.logger.info("process %s finished with status %s | nodesUsed %d", 
                                     self.config['prefix']+str(row[self.config['namecolumn']]), 
                                     proc_status, nodesUsed)

############################################################################
# query database for jobs to start
# no jobs will be started if the leftover walltime is less than runtime
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
                    if 'nodes' in  row.keys():
                        if nodesUsed + row['nodes'] > nnodes:
                            continue
                        nodelist = unused[:row['nodes']]
                        if len(nodelist) < row['nodes']:
                            break
                    else:
                        if nodesUsed >= nnodes:
                            break                        
                        nodelist = np.array([unused[0]])                    
                    #sys.stdout.flush()
                    # create directories, copy files
                    outdir = "%s/%s%d" % (self.config['outputpath'], self.config['prefix'], row[self.config['namecolumn']])
                    tmpdir = "%s/tmp" % (outdir)
                    outfile = "%s/%s%d%s" % (tmpdir, self.config['prefix'], row[self.config['namecolumn']], ".out")
                    mylib.mkdirs(tmpdir)
                    scriptname = os.path.basename(self.config['scriptname'])
                    scriptname = os.path.join(outdir, scriptname)
                    shutil.copyfile(self.config['scriptname'], scriptname)
                    shutil.copymode(self.config['scriptname'], scriptname)

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
                        ef.write('export %s=%s\n' % ('JM_HOSTFILE', hostfile) )
                        ef.write('#%s | %s\n' % (nodename, scriptname))
                    if self.config['remote']:
                        cmd = ["mpirun", "-genvnone", "-np", "1", "--host", nodename, scriptname, str(row[self.config['namecolumn']])]
                    else:
                        cmd = [scriptname, str(row[self.config['namecolumn']])]

                    # invoke script (on remote node if config['remote'] is set) and store process in dictionary, mark nodes as in use
                    Procs.append((Popen(cmd, cwd=outdir, stdout=PIPE, stderr=STDOUT, shell=False), row, nodelist))
                    for node in nodelist:
                        self.nodes[node]['used'] = True
                    status = row['status'] | PROCESSING
                    update.append((status, row['ID']))
                    nodesUsed += len(nodelist)
                    self.logger.info("process started %s", self.config['prefix']+str(row[self.config['namecolumn']]))

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
