#!/usr/bin/env python

import os
import datetime

cmd = './analyse_uao.py --day=DATE --side=R --flao --html --adseclogdir=/ao-data/adsecdx/aolog > output/DATE_DX.html'

start_date = datetime.date(2012, 12, 30)
end_date   = datetime.date(2017, 01, 31)

date_list = [ start_date + datetime.timedelta(n) for n in range(int ((end_date - start_date).days))]
dates = [x.strftime('%Y%m%d') for x in date_list]

for date in dates:
    mycmd = cmd.replace('DATE', date)
    print mycmd
    os.system(mycmd)
