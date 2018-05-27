#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 24 14:34:29 2018

@author: jiehuanhuang
"""
from dateutil.parser import parse

inactivity_period = open("./input/inactivity_period.txt", "r")
interval = int(inactivity_period.readline())

interval = 2

inactivity_period.close()

w = open("./output/sessionization.txt", "w")

main = dict()   # key: IP, value: list[start-time, start unix time, end-time, end unix-time, index]
time_ip = dict()    # key: last active unix-time, value: list of IPs 
count = dict()  # key: IP, value: set of IDs
active_time  = []   # list of end unix-time of live sessions
ip_order = []   # list of live IPs with order

#with open("../input/log20031231.csv", "r") as f:
with open("./input/log.csv", "r") as f:

    for i, line in enumerate(f):
        if i>0:
            s = line.split(',')
            ip = s[0]
            idt = s[4] + s[5] + s[6]
            time = s[1] +' '+ s[2]
            timestamp = parse(time).timestamp()

            # process expiring session
            if i==1:
                last_stamp = timestamp
                active_time.append(timestamp)

            if last_stamp != timestamp:
                last_stamp = timestamp
                for j in range(len(active_time)-1, -1, -1):
                    if (timestamp - active_time[j]) > interval:
                        expired_time = active_time[:j+1]
                        active_time = active_time[j+1:]
                        for k in range(len(expired_time)):
                            ip_list = time_ip.get(expired_time[k])
                           
                            del time_ip[expired_time[k]]
                           
                            for m in range(len(ip_list)):
                                tmp = main[ip_list[m]]
                                t = str(int(tmp[3] - tmp[1]+1))
                                c = str(len(count[ip_list[m]]))
                                res_line = (ip_list[m] + ','+ tmp[0] + ',' + tmp[2] + ','+ t +','+ c)
                                w.write(res_line)
                                w.write('\n')
                                del main[ip_list[m]]
                                del count[ip_list[m]]
                                ip_order.remove(ip_list[m])
                        
                        del expired_time    
                        break
                active_time.append(timestamp)


            # add new record
            if timestamp not in time_ip:
                tmp_list = []
   
            else:
                tmp_list = time_ip[timestamp]
            if ip not in tmp_list:
                tmp_list.append(ip)
                time_ip[timestamp] = tmp_list
                        
            if ip not in main:
                main[ip] = [time, timestamp, time, timestamp]
                count[ip] = set()
                ip_order.append(ip)
                
            else:
                main_tmp = main[ip]
                tmp_list = time_ip.get(main_tmp[3])
                if timestamp != main_tmp[3]:
                    tmp_list.remove(ip)
                    if len(tmp_list)==0:
                        del time_ip[main_tmp[3]]
                        active_time.remove(main_tmp[3])
                    else:
                        time_ip[main_tmp[3]] = tmp_list
                        
                    
                    main_tmp[2] = time
                    main_tmp[3] = timestamp
                    main[ip] = main_tmp
            count[ip].add(idt)
            

#finish final expired sessions
for n in range(len(ip_order)):
    tmp = main[ip_order[n]]
    t = str(int(tmp[3] - tmp[1]+1))
    c = str(len(count[ip_order[n]]))
    res_line = (ip_order[n] + ','+ tmp[0] + ',' + tmp[2] + ','+ t +','+ c)
    w.write(res_line)
    w.write('\n')

f.close()
w.close()