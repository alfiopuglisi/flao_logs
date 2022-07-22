#!/usr/bin/env python

import csv
import argparse
import numpy as np
import matplotlib.pyplot as plt

cmds = 'CompleteObs AcquireRefAO Acquire PresetAO CenterStar CenterPupils CheckFlux CloseLoop OptimizeGain ApplyOpticalGain OffsetXY'.split()

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

with open('cmd_%s.csv' % args.plot_side) as csvfile:
    datad = list(csv.reader(csvfile, delimiter=','))[1:]

alldates1 = [record[0] for record in datad]
alldates = []
[alldates.append(x) for x in alldates1 if x not in alldates]

#with open('cmd_succes%s.csv' % args.plot_side) as csvfile:
#    data = list(csv.reader(csvfile, delimiter=','))[1:]
with open('cmd_%s.csv' % args.plot_side) as csvfile:
    data = list(csv.reader(csvfile, delimiter=','))[1:]

selected_dates = [] #['20220308']
print "Date Command Attempts Successes Rate" 
for adate in alldates:
#    if len(selected_dates)>0:
#        cmddata = [record for record in data if record[0] in selected_dates]
#    else:
    cmddata = [record for record in data if record[0] >= args.start_date and record[0] <=args.end_date and record[0]==adate]
    for cmd in cmds:
        cmddata1 = [record for record in cmddata if record[2]==cmd]
        seconds = [int(record[3]) for record in cmddata1]
        for timing in seconds:
            print "%s %s %d" % (adate, cmd, timing)

