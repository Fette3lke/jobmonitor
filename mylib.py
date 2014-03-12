import sys
import os
import ast
import errno

def readCommandLine(args, delimiter="-", default="infilename"):
    """
    given a list of parameters [shortcut, name, default]
    these parameters are either set with their default value or with the command
    line argument following the shortcut or an environment variable of the same
    name if present.
    """

    cla = {"rcla_status": 1, "error": ""}
    for val in args:
        if val[2] in os.environ:
            cla[val[1]] = os.environ[val[2]]
        elif not val[2] == True:
            cla[val[1]] = val[2]

    i = 1
    while i < len(sys.argv):
        if not sys.argv[i][0] == delimiter:
            cla[default] = sys.argv[i]
            cla["rcla_status"] += 1
            i += 1
        else:
            for val in args:
                if sys.argv[i] == delimiter + val[0]:
                    if val[2]:
                        i += 1
                        cla[val[1]] = sys.argv[i]
                        cla["rcla_status"] += 1
                    else:
                        cla[val[1]] = 1
                        cla["rcla_status"] += 1
                    i += 1
                    break
            else:
                return {"rcla_status": 0, "error": "unknown option %s" % sys.argv[i]}
    return cla


def usage(args, delimiter="-"):
    for v in args:
        if len(v) > 3:
            print >> sys.stderr, "\t", delimiter + v[0], "\t", v[3]
        else:
            print >> sys.stderr, "\t", delimiter + v[0], "\t", v[1]
    return 0


def readConfigFile(filename):
    if not os.path.exists(filename):
        return {}
    dict = {}
    with open(filename, "r") as f:
        dict = {}
        lines = f.read().split("\n")
        for l in lines:
            if not l or l[0] == '#':
                continue
            v = l.split(None)
            if len(v) >= 2:
                dict[v[0]] = ast.literal_eval(v[1])
    return dict

def mkdirs(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise
