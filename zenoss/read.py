#!/usr/bin/python

#
# Reads the most recent value from the provided RRD file.  Prints this value to stdout and exits.  Exits with non-zero return code on error.
#
# Contract with return codes:
#    0: good data (yay)
#    1: stale value (old data in rrd file)
#    2: file not found
#

import sys
import time
import os
import getopt

DEFAULT_INTERACTIVE = False

def fetch(filename):
    '''read the last metric from the filename provided.'''
    import rrdtool

    fopts = []
    fopts.append("%s" % filename)
    fopts.append("AVERAGE")
    results = rrdtool.fetch(*fopts)
    
    points = results[-1]
    for pos in range(2, 9):
        loc = pos * -1
        raw = points[loc][0]
        if raw != None:
            return raw

    return None

def polish(f):
    '''Polishes the raw numeric value:
       - truncates to twelve significant digits
       - fixes errant values (like negative 0)

       If the value is None, then None is returned.'''

    if f == None:
        return None

    f = round(f, 12)
    if f == -0.0:
        f = abs(f)

    return f

def read(filename):
    return polish(fetch(filename))

def read_and_print(filename):
    if not os.path.exists(filename):
        sys.stdout.write("no such file: %s\n" % Filename)
        sys.stdout.flush()
        return

    # fetch and polish the value
    val = polish(fetch(filename))
    #val = 5.0

    # exit with 1 if the value could not be read
    if val == None:
        sys.stdout.write("value could not be read\n")
    else:
        # Java expects the value to on stdout
        sys.stdout.write('%s\n' % val)

    sys.stdout.flush()


def main():
    interactive = DEFAULT_INTERACTIVE
    files = []

    try:
        opts, args = getopt.gnu_getopt(sys.argv[1:], ':h', ['help', 'interactive'])

        for o, a in opts:
            if o in ("-h", "--help"):
                usage()
            elif o in ("--interactive"):
                interactive = True

        files = args[0:]

    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    for filename in files:
        read_and_print(filename)

    if interactive:
        while True:
            line = sys.stdin.readline()

            if (".\n" == line):
                break

            filename = line[:-1]
            read_and_print(filename)

if __name__ == '__main__':
    main()

    
