#!/usr/bin/env python

import os
import datetime

cmd = './analyse_uao.py --day=DATE --side=R --html --adseclogdir=/local/aolog > output/DATE_DX.html'

start_date = datetime.date(2020, 05, 01)
end_date   = datetime.date(2020, 12, 31)

date_list = [ start_date + datetime.timedelta(n) for n in range(int ((end_date - start_date).days))]
dates = [x.strftime('%Y%m%d') for x in date_list]

print dates

for date in dates:
    mycmd = cmd.replace('DATE', date)
    print mycmd
    os.system(mycmd)
