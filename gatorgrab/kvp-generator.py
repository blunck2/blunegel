#!/usr/bin/python2.7

import read, socket, binascii, msgpack, json, urllib2

import os, time
from xml.dom.ext import PrettyPrint
from xml.dom.ext.reader import Sax2
from StringIO import StringIO
import string

from pprint import pprint

ROOT = '/opt/zenoss/perf/Devices/'
INTERVAL = 300

def listdir(path):
    dirs = []
    if not os.path.exists(path):
        return []
    
    for child in os.listdir(path):
        if child in [".", "", "\n"]:
            continue

        full_path = path + "/" + child
        
        if os.path.isdir(full_path):
            dirs.append(full_path)

    return dirs

def listrrd(path):
    rrds = []
    for child in os.listdir(path):
        if child in [".", "", "\n"]:
            continue
        
        full_path = path + "/" + child

        if os.path.isfile(full_path) and full_path.endswith('.rrd'):
            rrds.append(full_path)

    return rrds

def get_pretty_timestamp():
    prefix = time.strftime("%Y-%m-%dT%H:%M:%S.000")

    tz = time.strftime("%z")
    tz_hours = tz[0:3]
    tz_mins = tz[3:]
    suffix = '%s:%s' % (tz_hours, tz_mins)

    return '%s%s' % (prefix, suffix)

def get_timestamp():
    return int(time.time())


def collect_interfaces(device, device_path):
    root = device_path + "/os/interfaces"
    interfaces = listdir(root)
    if len(interfaces) == 0:
        return None

    ts = get_timestamp()

    interface = os.path.basename(interfaces[0])
    rrds = listrrd(root + "/" + interface)

    all_output = ""

    for rrd in rrds:
        millis = INTERVAL * 1000
        name = os.path.basename(rrd)

        for interface in interfaces:
            interface = os.path.basename(interface)
            try:
                filename = root + "/" + interface + "/" + os.path.basename(rrd)
                value = read.read(filename)
                
                output = "%s interfaces/%s/%s %d %s\n" % (device, interface, os.path.basename(rrd), ts, int(value))
                all_output += output
            except:
                pass

    return all_output


def convert_hostname_to_slashes(hostname):
    parts = hostname.split(".")

    # handle "localhost" scenario
    if len(parts) == 1:
        return hostname
    
    # handle ip address (not hostname) scenario
    if len(parts) == 4:
        if parts[0].isdigit() and parts[1].isdigit() and parts[2].isdigit() and parts[3].isdigit():
            return hostname.replace(".", "/")
    
    parts.reverse()
    
    return "/" + string.join(parts, "/")

    
def collect(device_path):
    rrds = listrrd(device_path)

    if len(rrds) == 0:
        return

    hostname = device_path.split('/')[-1]
    ts = get_timestamp()

    all_output = ""

    hit = False

    for rrd in rrds:
        millis = INTERVAL * 1000
        name = os.path.basename(rrd)
        name = name.replace(" ", "_")
        value = read.read(rrd)
        if value is None:
            print 'WARN: %s has None value' % rrd
            continue

        output = "%s %s %d %s\n" % (hostname, name, ts, int(value))
        all_output += output


        hit = True

    interfaces = collect_interfaces(hostname, device_path)
    if interfaces is not None:
        all_output += interfaces

    if hit:
        return all_output
    else:
        return None


def collect_cycle():
    devices = listdir(ROOT)

    all_measurements = ""
    
    for device in devices:
        measurements = collect(device)
        if measurements is not None:
            all_measurements += measurements

    return all_measurements

def convert_to_slashes(key):
    key = key.replace(".", "/")
    key = key.replace("_", "/")
    key = key.replace("-", "/")
    return key

def clean_key(key):
    clean = key.replace(".rrd", "")
    clean = convert_to_slashes(clean)
    return clean

def send_to_skyline(results, hostname, port):
    fp = open("/tmp/keys.csv", "w")

    for result in results.split('\n'):
        if len(result) == 0: 
            continue

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((hostname, port))

        (host, metric_name, ts, value) = result.split()
        key = "%s/%s" % (convert_hostname_to_slashes(host), metric_name)
        key = clean_key(key)
        fp.write("%s\n" % key)
        print "(skyline) %s => %s" %  (key, str(value))
        
        message = msgpack.packb((key, [int(ts), float(value)]))

        sock.sendall(message)
        sock.close()

    fp.close()

def send_to_mongo(results, hostname, port):
    all_metrics = []
    for result in results.split('\n'):
        if len(result) == 0:
            continue

        (metric_hostname, metric_name, ts, value) = result.split()
        key = clean_key("%s" % metric_name)

        metric_basename = metric_hostname.split(".")[0]
        metric_domainname = string.join(metric_hostname.split(".")[1:], ".")
        metric = { "network" : metric_domainname, 
                   "host" : metric_hostname, 
                   "observer" : "zenoss", 
                   "key" : key, 
                   "value" : float(value), 
                   "timestamp": int(ts)}
        all_metrics.append(metric)

        
    j = json.dumps(all_metrics)
            
    req = urllib2.Request("http://%s:%s/rs/ingest" % (hostname, port))
    req.add_header('Content-Type', 'application/json')
    req.add_data(j)
    f = urllib2.urlopen(req)
    response = f.read()
    f.close()

    print "(mongo) %s (%s)" % (j, response)
        

def main():
    results = collect_cycle()
    send_to_skyline(results, '192.168.100.10', 2025)
    send_to_mongo(results, '192.168.100.10', 9494)
        

if __name__ == '__main__':
    main()

