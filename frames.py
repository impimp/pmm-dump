#!/usr/bin/python
import json
import sys
import struct
import datetime
import gzip
import glob 
from os.path import basename


def httpdate(dt):
    weekday = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]
    month = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep",
             "Oct", "Nov", "Dec"][dt.month - 1]
    return "%s, %02d %s %04d %02d:%02d:%02d GMT" % (weekday, dt.day, month,
        dt.year, dt.hour, dt.minute, dt.second)


def Frame(name, url,timestamp, data):
	magic = [ 0x83, 0xF1, 0xF1 ] 
	version = [ 0x00, 0x00, 0x00]
	magic = map(chr, magic)
	version = map(chr, version)
	f = ''.join(magic) + ''.join(version);
	datasize = 222;
	f = f + b'\0' * 6;
	f = f + struct.pack(">I", len(data));
	f = f + b'\0'* 4
	
	f = f + struct.pack(">I", timestamp);
	name = "pc-374.home"
	# u = 'http://127.0.0.1:8000/metrics-lr'
	f = f + name;
	f = f + b'\0' * (52-len(name))
	f = f + url;
	f = f + b'\0' * (52-len(url))
	f = f + data
	return f


def Payload(name, labels, value, timestamp):
	date = datetime.datetime.fromtimestamp(timestamp)

	_labels = ', '.join(['{}="{}"'.format(k,v) for k,v in labels.iteritems()])

	return '''HTTP/1.0 200 OK
Content-Type: text/plain; version=0.0.4
Date: {}


{}{{{}}} {}

'''.format(httpdate(date),name,_labels,value)
	

g = glob.glob("dump/*.gz")
for f in g:
	print "Next file={}".format(f)
	with gzip.open(f,'r') as handle:
		for line in handle:
			try:
				j = json.loads(line)
				for result in j['data']['result']:
					name = result['metric']['__name__'] 
					del result['metric']['__name__'] 
					file = open('frames/{}.frame'.format(basename(f).replace('.gz','')), 'wb')
					for value in result['values']:
						file.write(
							Frame(
								name, 
								'http://127.0.0.1/fakeMetrics', 
								value[0],
								Payload(
									name,
									result['metric'], 
									value[1], 
									value[0])
								)
							)
					file.close()				
			except:
				print (sys.exc_info()[0],sys.exc_info()[1])
