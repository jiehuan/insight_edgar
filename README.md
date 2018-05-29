# insight_edgar

# Approach

The following dictionaries and lists are used for processing.

    main = dict()		# key: IP, value: list[start-time, start unix time, end-time, end unix-time]
    time_ip = dict()	# key: last active unix-time, value: list of IPs each in sequential orders
    count = dict()  	# key: IP, value: set of IDs
    active_time  = []	# list of end unix-time of live sessions
	
![Alternate image text](https://github.com/jiehuan/insight_edgar/blob/master/image/flow.png)


# Requirements

This solution is code in Python 3.6.5. The following packages need to be imported.

	import sys
	import pandas as pd
	from dateutil.parser import parse
	

# Run Instructions

execute 'run.sh' in terminal

./run.sh


## default file names and locations

Python file:			./src/sessionization.py 
Input csv file:			./input/log.csv 
Inactivity period file:	./input/inactivity_period.txt 
Output file:			./output/sessionization.txt

If file names or locations need to be changed, the command in run.sh should also be changed accordingly.
