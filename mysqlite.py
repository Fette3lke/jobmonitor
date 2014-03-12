import os
import sys
import sqlite3


def db_init(dbname=None):
    if dbname == None:
        dbname = os.getenv('DB')
    if not dbname:
        print "Database: "
        dbname = sys.stdin.readline()
    if os.path.exists(dbname):
        conn = sqlite3.connect(dbname)
    else:
        sys.stderr.write("File does not exist: %s \n" % dbname)
        return 0
    conn.row_factory = sqlite3.Row
    return conn
