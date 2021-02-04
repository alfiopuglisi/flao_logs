#!/usr/bin/env python

import csv
import argparse
import numpy as np
import matplotlib.pyplot as plt

cmds = 'PresetAO CenterStar CenterPupils CheckFlux CloseLoop OptimizeGain ApplyOpticalGain OffsetXY'.split()

parser = argparse.ArgumentParser()
parser.add_argument('--start', dest='start_date', action='store',
                               default='19000101', help='Start date (YYYYMMDD)')
parser.add_argument('--end',   dest='end_date', action='store',
                               default='21000101', help='End date (YYYYMMDD)')
parser.add_argument('--name',  dest='plot_name', action='store',
                               default='', help='Plot name')
parser.add_argument('--side',  dest='plot_side', action='store',
                               help='side (L or R)')

args = parser.parse_args()

html = '<H1>%s, side=%s</H1>' % (args.plot_name, args.plot_side)

with open('cmd_%s.csv' % args.plot_side) as csvfile:
    data = list(csv.reader(csvfile, delimiter=','))[1:]

maxtime = 0
for cmd in cmds:
    cmddata = [record for record in data if record[0] >= args.start_date and record[0] <=args.end_date]
    cmddata = [record for record in cmddata if record[2] == cmd]
    times = [float(record[3]) for record in cmddata]
    if len(times) == 0:
        continue
    maxtime = max([max(times), maxtime])

for cmd in cmds:
    cmddata = [record for record in data if record[0] >= args.start_date and record[0] <=args.end_date]
    cmddata = [record for record in cmddata if record[2] == cmd]
    times = [float(record[3]) for record in cmddata]
    if len(times) == 0:
        continue
    filename = 'plot_%s_%s_%s.png' % (args.plot_side, args.plot_name, cmd.lower())

    plt.clf()
    plt.hist(times, bins=np.arange(((maxtime+5)/5))*5)
    plt.title(cmd)
    plt.xlabel('Elapsed time (s)')
    plt.ylabel('Occurrences')
    plt.savefig(filename)

    html += '<img src="%s">\n' % filename

with open('plots_%s_%s.html' % (args.plot_side, args.plot_name), 'w') as f:
    f.write(html)
     

