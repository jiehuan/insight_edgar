#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 24 14:34:29 2018

@author: jiehuanhuang
"""
import sys
import pandas as pd
from dateutil.parser import parse

def sessionization(filein, inactive, fileout):

    try:
        inactivity_period = open(inactive, "r")
        interval = int(inactivity_period.readline())
        inactivity_period.close()
    except IOError:
        print ('Cannot open inactivity_period file.')
        return
        
    w = open(fileout, "w")
    
    main = dict()       # key: IP, value: list[start-time, start unix time, end-time, end unix-time]
    time_ip = dict()    # key: last active unix-time, value: list of IPs each in sequential orders
    count = dict()      # key: IP, value: set of IDs
    active_time  = []   # list of end unix-time of live sessions
    last_stamp = parse('1970-1-1').timestamp()
    
    try:   
        with open(filein, "r") as f:
            for i, line in enumerate(f):
                if i>0:     # skip the header line
                    try:
                        s = line.split(',')
                        ip = s[0]
                        idt = s[4] + s[5] + s[6]    # use as identification for each request
                    except:
                        print ('Read line error. Line is skipped')
                        continue
                    try:
                        time = s[1] +' '+ s[2]
                        timestamp = parse(time).timestamp() # convert time to unix time
                    except:
                        print ('Cannot parse time. Line is skipped')
                        continue
    
                    # process expiring session
                    if last_stamp != timestamp:
                        last_stamp = timestamp
    
                        # write expired sessions to fileout
                        for j in range(len(active_time)-1, -1, -1):
                            if (timestamp - active_time[j]) > interval:
    
                                expired_time = active_time[:j+1]        # remove expired timestamp from active_time
                                active_time = active_time[j+1:]
                                for k in range(len(expired_time)):
                                    ip_list = time_ip[expired_time[k]]  # get the list of IPs at the expired timestamp
    
                                    del time_ip[expired_time[k]]        # del this timestamp from time_ip dict
    
                                    for m in range(len(ip_list)):       # write each IP to fileout
                                        tmp = main[ip_list[m]]
                                        t = str(int(tmp[3] - tmp[1]+1))
                                        c = str(len(count[ip_list[m]]))
                                        res_line = (ip_list[m] + ','+ tmp[0] + ',' + tmp[2] + ','+ t +','+ c)
                                        w.write(res_line)
                                        w.write('\n')
                                        # del this IP in main and count
                                        del main[ip_list[m]]
                                        del count[ip_list[m]]
    
                                del expired_time
                                break
                        active_time.append(timestamp)   # add current timestamp to active_time list
    
    
                    # add new record
                    if timestamp not in time_ip:    # add new timestamp of this request to time_ip dict
                        tmp_list1 = []
                    else:
                        tmp_list1 = time_ip[timestamp]
    
                    if ip not in tmp_list1:
                        tmp_list1.append(ip)
                        time_ip[timestamp] = tmp_list1
    
                    if ip not in main:              # add or update this request to main and count
                        main[ip] = [time, timestamp, time, timestamp, i]
                        count[ip] = set()
    
                    else:
                        main_tmp = main[ip]
                        tmp_list2 = time_ip[main_tmp[3]]
    
                        if timestamp != main_tmp[3]:
                            tmp_list2.remove(ip)
                            if len(tmp_list2)==0:
                                del time_ip[main_tmp[3]]
                                active_time.remove(main_tmp[3])
                            else:
                                time_ip[main_tmp[3]] = tmp_list2
    
                            main_tmp[2] = time
                            main_tmp[3] = timestamp
                            main[ip] = main_tmp
    
                    count[ip].add(idt)          # add request identification to count dict
    except IOError:
        print ('Cannot read input file')
        return
    
    w.close()

    #write all un-expired sessions to file
    
    df = pd.DataFrame(columns=['IP', 'start_time', 'end_time', 't', 'c', 'index'])

    for item in main:
        tmp = main[item]
        t = str(int(tmp[3]-tmp[1]+1))
        c = len(count[item])
        df = df.append({'IP': item, 'start_time': tmp[0], 'end_time': tmp[2], 't': t, 'c': c, 'index': tmp[4]}, ignore_index=True)

    df = df.set_index('IP')
    df = df.sort_values(by='index')
    df = df.drop('index', axis=1)

    df.to_csv(fileout, header=False, mode = 'a')
    
    
if __name__ == '__main__':
    filenames = sys.argv
    filein = filenames[1]
    inactive = filenames[2]
    fileout = filenames [3]

    sessionization(filein, inactive, fileout)
