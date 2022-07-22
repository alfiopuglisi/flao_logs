#!/usr/bin/env python

import os
import datetime

cmd = './analyse_uao.py DATE L /ao-data/sxadsec/Log_and_Telemetry > output/DATE_SX.txt'
#cmd = './analyse_uao.py DATE L /local/aolog > output/DATE_SX.txt'

start_date = datetime.date(2021, 03, 01)
end_date   = datetime.date(2022, 05, 14)

date_list = [ start_date + datetime.timedelta(n) for n in range(int ((end_date - start_date).days))]
dates = [x.strftime('%Y%m%d') for x in date_list]

for date in dates:
    mycmd = cmd.replace('DATE', date)
    print mycmd
    os.system(mycmd)

cmd = './analyse_uao.py DATE L /local/aolog > output/DATE_SX.txt'
start_date = datetime.date(2022, 05, 14)
end_date   = datetime.date(2022, 06, 21)

date_list = [ start_date + datetime.timedelta(n) for n in range(int ((end_date - start_date).days))]
dates = [x.strftime('%Y%m%d') for x in date_list]

for date in dates:
    mycmd = cmd.replace('DATE', date)
    print mycmd
    os.system(mycmd)

