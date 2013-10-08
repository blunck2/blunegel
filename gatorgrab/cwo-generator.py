#!/usr/bin/python

import read

import os, time
from xml.dom.ext import PrettyPrint
from xml.dom.ext.reader import Sax2
from StringIO import StringIO

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

def get_timestamp():
    prefix = time.strftime("%Y-%m-%dT%H:%M:%S.000")

    tz = time.strftime("%z")
    tz_hours = tz[0:3]
    tz_mins = tz[3:]
    suffix = '%s:%s' % (tz_hours, tz_mins)

    return '%s%s' % (prefix, suffix)


def collect_interfaces(device_path):
    root = device_path + "/os/interfaces"
    interfaces = listdir(root)
    if len(interfaces) == 0:
        return None

    interface = os.path.basename(interfaces[0])
    rrds = listrrd(root + "/" + interface)

    xml = ''

    for rrd in rrds:
        millis = INTERVAL * 1000
        name = os.path.basename(rrd)

        mvnm = '<MultiValueNumericMeasurement intervalInMilliseconds="%d" measurementName="%s" measurementType="COUNTER" units="unknown">' % (millis, name)

        numerics = "<numericValues>"
        for interface in interfaces:
            interface = os.path.basename(interface)
            try:
                filename = root + "/" + interface + "/" + os.path.basename(rrd)
                value = read.read(filename)
                
                numeric = '<numericValue key="%s">%f</numericValue>' % (interface, value)
                numerics += numeric
            except:
                pass
        numerics += "</numericValues>"

        mvnm += numerics + "</MultiValueNumericMeasurement>"
        
        xml += mvnm

    return xml

    
def collect(device_path):
    rrds = listrrd(device_path)

    if len(rrds) == 0:
        return

    hostname = device_path.split('/')[-1]
    ts = get_timestamp()
    
    xml = '<ServerMeasurement xmlns="http://mms/cwo/types/1.2.0" origin="generator.py" site="Blunck WAN" state="UP" hostname="%s" timestamp="%s">' % (hostname, ts)

    hit = False

    for rrd in rrds:
        millis = INTERVAL * 1000
        name = os.path.basename(rrd)
        value = read.read(rrd)
        if value is None:
            continue
        
        numeric = '<NumericMeasurement intervalInMilliseconds="%d" measurementName="%s" measurementType="RATE" units="unknown" value="%f"/>' % (millis, name, value)
        xml += numeric

        hit = True

    mvnm = collect_interfaces(device_path)
    if mvnm is not None:
        xml += mvnm

    xml += "</ServerMeasurement>"

    if hit:
        return xml
    else:
        return None

def toprettyxml(node, encoding='utf-8'):
    tmp = StringIO()
    PrettyPrint(node, stream=tmp, encoding=encoding)
    return tmp.getvalue()


def collect_cycle():
    devices = listdir(ROOT)

    cwo = '<?xml version="1.0" encoding="UTF-8"?><ServerMeasurements xmlns="http://mms/cwo/1.2.0">'
    
    for device in devices:
        server_measurement = collect(device)
        if server_measurement is not None:
            cwo += server_measurement


    cwo += "</ServerMeasurements>"

    reader = Sax2.Reader()
    doc = reader.fromString(cwo)
    pretty = toprettyxml(doc)

    return pretty


def main():
    xml = collect_cycle()
    print xml
        

if __name__ == '__main__':
    main()

