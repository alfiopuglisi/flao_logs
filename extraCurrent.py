#!/usr/bin/env python

import os
import datetime


date_time = '''
2019-01-21 10:17
2019-01-21 10:52
2019-03-08 18:44
2019-03-11 17:46
2019-03-14 01:16
2019-04-22 07:03
2019-05-07 08:26
2019-05-24 04:55
2019-05-24 05:29
2019-05-26 04:24
2019-06-07 10:08
2019-06-27 22:38
2019-07-06 16:44
2019-07-11 05:19
2019-09-09 18:40
2019-09-09 20:22
2019-09-13 04:45
2019-10-11 07:41
2019-10-17 03:00
2019-10-22 17:55
2019-11-06 17:12
2019-11-06 17:13
2019-11-06 17:14
2019-11-06 21:08
2019-11-07 00:34
2019-11-07 00:35
2019-11-07 00:36
2019-11-07 16:34
2019-11-08 04:47
2019-11-08 10:19
2019-11-08 10:28
2019-11-08 17:02
2019-11-08 17:03
2019-11-08 17:04
2019-11-10 21:23
2019-11-10 21:24
2019-11-10 21:25
2019-11-11 00:21
2019-11-11 02:39
2019-11-11 02:40
2019-11-11 02:41
2019-12-09 20:45
2019-12-13 03:39
2019-12-15 01:29
2020-01-08 02:40
2020-01-09 06:53
2020-01-13 02:14
2020-01-13 12:49
2020-01-27 03:21
2020-02-07 12:50
2020-02-08 20:18
2020-02-27 19:58
2020-03-14 07:39
2020-06-07 06:36
2020-06-11 17:24
2020-06-17 11:39
2020-06-27 07:27
2020-07-07 03:09
2020-07-09 04:21
2020-09-27 09:26
2020-09-30 03:34
2020-10-01 06:20
2020-10-03 02:47
2020-10-10 22:09
2020-10-30 03:34
2020-11-04 05:56
2020-11-27 02:49
2020-11-30 12:07
2020-12-01 07:08
2020-12-08 01:30
2020-12-08 01:52
2020-12-08 01:53
2020-12-19 10:03
2020-12-21 04:31
2020-12-21 05:04
2020-12-26 03:38
2020-12-31 05:35
2020-12-31 08:39
2021-01-09 01:16
2021-01-22 16:45
2021-02-23 02:41
2021-02-24 02:03
2021-02-25 02:13
2021-03-01 11:19
2021-03-31 05:48
2021-04-23 06:11
2021-04-25 10:31
2021-04-30 03:00
2021-05-26 06:36
2021-05-28 09:47
2021-05-29 08:07
2021-05-30 09:29
2021-05-31 06:26
2021-06-21 23:07
2021-06-22 16:52
2021-09-22 21:01
2021-09-23 22:42
2021-10-09 10:57
2021-10-19 19:46
2021-10-24 04:19
2021-10-27 08:05
2021-11-26 17:19
2021-12-09 03:08
2021-12-17 02:55
2022-01-28 09:49
2022-02-08 09:49
2022-02-09 11:49
2022-02-15 06:09
2022-02-20 12:38
2022-03-15 12:52
2022-03-23 02:27
2022-03-23 07:50
2022-04-15 03:02
2022-04-18 07:57
2022-04-18 08:13
2022-05-11 18:07
2022-05-12 06:45
2022-05-13 09:47
'''


# cmd = 'zgrep TIME: /ao-data/sxsoul/Log_and_Telemetry/YYYY/MM/DD/housekeeperWFS.L.*.log.gz | head -n 1'
#cmd = 'zgrep TIME: /ao-data/sxsoul/Log_and_Telemetry/YYYY/MM/DD/housekeeperWFS.L.*.log.gz | cut -c 148- | head -n 1'
cmd = 'zgrep TIME: /ao-data/sxsoul/Log_and_Telemetry/YYYY/MM/DD/housekeeperWFS.L.*.log.gz | grep "HW frame rate" |  cut -c 158-166 | head -n 1'
cmd_a = 'zgrep TIME: /ao-data/sxsoul/Log_and_Telemetry/YYYY/MM/DD/housekeeperWFS.L.*.log.gz'
cmd_b = 'zgrep TIME: /ao-data/sxsoul/Log_and_Telemetry/YYYY/MM/DD/housekeeperWFS.L.*.log.gz'

cy = c0 = c1 = 0
yycy = {}
yyc0 = {}
yyc1 = {}

years = ['2019', '2020', '2021', '2022']
for yy in years: 
    yycy[yy] = 0
    yyc0[yy] = 0
    yyc1[yy] = 0

for ll in date_time.split('\n'):
    if ll:
        adate, atime = ll.split(' ')
#        print adate, atime
        yy, mm, dd = adate.split('-')
        mycmd = cmd.replace('TIME', atime)
        mycmd = mycmd.replace('YYYY', yy)
        mycmd = mycmd.replace('MM', mm)
        mycmd = mycmd.replace('DD', dd)
        mycmd_a = cmd_a.replace('TIME', atime)
        mycmd_a = mycmd_a.replace('YYYY', yy)
        mycmd_a = mycmd_a.replace('MM', mm)
        mycmd_a = mycmd_a.replace('DD', dd)
#        print(mycmd)
#        os.system(mycmd)
        out = os.popen(mycmd).read()
        if out:
            print adate, atime, ':', out
            ff = float(out)
            print float(ff)
            if ff>=1000.0:
                c1 +=1
                yyc1[yy] += 1
            else:
                c0+=1
                yyc0[yy] += 1
        else:
            out = os.popen(mycmd_a).read()
            print out
            cy +=1
            yycy[yy] += 1


print "Greater or equal than 1000.0: ", c1
print "Less than 1000.0: ", c0
print "System not used by SOUL-LUCI SX: ", cy
print c1, c0, cy
print

for yy in years: 
    print yy
    print "Greater or equal than 1000.0: ", yyc1[yy]
    print "Less than 1000.0: ", yyc0[yy]
    print "System not used by SOUL-LUCI SX: ", yycy[yy]
    print yy, yyc1[yy], yyc0[yy], yycy[yy]

