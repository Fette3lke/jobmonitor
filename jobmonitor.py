import sys
import os
import mylib
import mysqlite
import time
import math
import re
import logging
from subprocess import *

# common flags
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
            ["n", "max_ic_procs", True],
            ["l", "logfile", True],
            ["lvl", "loglvl", True]
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
                         ["mpi_threads", 1],
                    #            ["scriptdir", None],
                         ["scriptname", None],
                         ["database", None],
                    #            ["parampath", None],
                         ["outputpath", None],
                         ["prefix", ""],
                         ["max_ic_procs", 1],
                         ["walltime", None],
                         ["refinetime", 900],
                         ["polltime", 60],
                         ["table", None],
                         ["namecolumn", "ID"],
                         ["flag_marked", 1],
                         ["flag_success", 2],
                         ["logfile", "./log.txt"],
                         ["loglvl", "WARNING"]                       
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

    def test(self):
        for key in self.config:
            os.environ['JM_'+key.upper()] = str(self.config[key])
        outf = open('test', 'a')
        print self.config['scriptname']
        Popen([self.config['scriptname'], str('testing123')], stdout=outf, stderr=STDOUT)
        outf.close()

    def loop(self):
        # Select halos that are marked and start to create ICs for them -> flag PROCESSING
        limit = math.floor(self.config['max_ic_procs'] / (self.config['mpi_threads']))
        if limit < 1:
            limit = 1
        refineProcs = []
        procsUsedRefine = 0
        for key in self.config:
            os.environ['JM_'+key.upper()] = str(self.config[key])

        # main loop, creating and observing jobs
        while 1:
            update = []

            # poll jobs, check whether they have finished, update database accordingly
            for proc, row in refineProcs[:]:
        #        output, err = proc.communicate()
                if proc.poll() is not None:
                    outdir = "%s/%s%d" % (self.config['outputpath'], self.config['prefix'], row[self.config['namecolumn']])
                    tmpdir = "%s/tmp" % (outdir)
                    outfile = "%s/%s%d%s" % (tmpdir, self.config['prefix'], row[self.config['namecolumn']], ".out")
                    outf = open(outfile, 'r')
                    output = outf.readlines()
                    outf.close()                    
                    # script to generate ICs should finish with string 'SUCCESS'
                    proc_status = "FAIL"
                    if re.search("SUCCESS", output[-1]) is not None:
                        status = (row['status'] | self.config['flag_success']) & ~self.config['flag_marked'] & ~PROCESSING
                        update.append((status, row['ID']))
                        proc_status = "SUCCESS"
                        if self.onSuccess:
                            self.onSuccess(self.config, row)
                    else:
                        outf = open(outfile, 'a')
                        out = proc.communicate()[0]
                        outf.write(out)
                        outf.close()

                    procsUsedRefine -= (self.config['mpi_threads'])
                    refineProcs.remove((proc, row))
                    self.logger.info("process %s finished with status %s | procsUsed %d", self.config['prefix']+str(row[self.config['namecolumn']]), proc_status, procsUsedRefine)

            if ((time.time() - self.config['startup']) < (self.config['walltime'] - self.config['refinetime'])):
                query = self.dbcur.execute("SELECT * FROM {0} WHERE status=:marked AND NOT status & :busy LIMIT :limit".format(self.config['table']), {"limit": limit, "marked": self.config['flag_marked'], "busy": BUSY})
#                self.logger.debug("time passed: %d | max time: %d", (time.time() - self.config['startup']), (self.config['walltime'] - self.config['refinetime'])) 

                # start new jobs
                for row in query:
                    if procsUsedRefine + self.config['mpi_threads'] > self.config['max_ic_procs']:
                        break
                    outdir = "%s/%s%d" % (self.config['outputpath'], self.config['prefix'], row[self.config['namecolumn']])
                    tmpdir = "%s/tmp" % (outdir)
                    mylib.mkdirs(tmpdir)
                    refineProcs.append((Popen([self.config['scriptname'], str(row[self.config['namecolumn']])], cwd=outdir, stdout=PIPE, stderr=STDOUT), row))
                    status = row['status'] | PROCESSING
                    update.append((status, row['ID']))
                    procsUsedRefine += self.config['mpi_threads']
                    self.logger.info("process started %s", self.config['prefix']+str(row[self.config['namecolumn']]))

            if update:
                self.dbcur.executemany("UPDATE {0} SET STATUS=? WHERE ID=?".format(self.config['table']), update)
                self.db.commit()
                update = []

            if len(refineProcs) > 0:
                self.logger.debug("processes: %d | procs used %d", len(refineProcs), procsUsedRefine)
                time.sleep(self.config['polltime'])
            else:
                break

        # mark all halos that are still processing as erroneous
        self.logger.debug('main loop finished')
        self.dbcur.execute("UPDATE {0} SET status = status | ? WHERE status & ? and status & ?".format(self.config['table']), (ERROR, self.config['flag_marked'], PROCESSING))
        self.db.commit
        return 0
