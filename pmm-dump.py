#!/usr/bin/python
from optparse import OptionParser
import sys
import time
import requests
from datetime import datetime
import gzip
import multiprocessing

def itlist(j):
    pass

def main():
    parser = OptionParser(usage="usage: %prog [options]",
                          version="%prog 0.1")
    parser.add_option("-s", "--start",
                      action="store",
                      default=int(time.time()-1201), 
                      dest="startTime",
                      help="startTime",)

    parser.add_option("-e", "--end",
                      action="store", 
                      default=int(time.time()),
                      dest="endTime",
                      help="endTime",)

    parser.add_option("-H", "--host",
                      action="store", 
                      dest="host",
                      help="host",)

    parser.add_option("-P", "--port",
                      action="store", 
                      dest="port",
                      help="port",)

    parser.add_option("-u", "--username",
                      action="store", 
                      dest="username",
                      help="username",)

    parser.add_option("-p", "--password",
                      action="store", 
                      dest="password",
                      help="password",)


    options = parser.parse_args()[0]


    if type(options.startTime) is str:
        # ...
        pass
    elif type(options.startTime) is int:
        startTime = int(options.startTime);
    else:
        # ...
        pass

    if type(options.endTime) is str:
        # ...
        pass
    elif type(options.endTime) is int:
        endTime = int(options.endTime)
    else:
        pass

    if (endTime - startTime) < 600:
      print "endTime - startTime < 600";
      sys.exit(1)

    response = requests.get('http://{}/prometheus/api/v1/label/__name__/values'.format(options.host), auth=(options.username,options.password),
                            headers={'Content-Type': 'application/json'})

    if response.status_code != 200:
      print "HTTPCode: {} for __name__/values".format(response.status_code)
      sys.exit(1)
    
    pool = multiprocessing.Pool(processes=5)
    results_of_processes = [pool.apply_async(
       getData, 
      args=(options, serie,startTime, endTime),
      callback=None
  ) for serie in response.json()['data']]

    pool.close()
    pool.join()

def getData(options, serie, s, e):
    gz = gzip.open('dump/s_{}.gz'.format(serie), 'w', 5)
    ss = s + 600
    while ss <= e:
        print >> sys.stderr, 'Dumping {} to {}'.format(serie, datetime.fromtimestamp(ss).strftime('%Y-%m-%d %H:%M:%S'))

        response = requests.get('http://192.168.1.37/prometheus/api/v1/query?query={}[{}]&time={}'.format(serie,'10m',ss), auth=('percona','percona'))
        if response.status_code != 200:
          print >> sys.stderr, 'Serie: {} {} failed'.format(serie,ss)
        else:
          gz.write(response.text + '\n')
        ss = ss + 600
    gz.close()


if __name__ == '__main__':
    main()