#!/usr/bin/env python

import csv
import os, glob, time, getopt, sys, calendar, re, math
import operator, fileinput

def usage():
    print 'Usage:'
    print 'analyse.py --day=YYYYMMDD --side=[R|L][-v] [--html] [--flao] [--summary="summary.txt"] [--wfslogdir="log directory"] [--adseclogdir="log directory"] [--dataoutdir="data out directory"]'

try:
    optlist, args = getopt.getopt( sys.argv[1:], 'v', ['html', 'flao', 'day=', 'side=', 'summary=', 'wfslogdir=', 'adseclogdir=', 'dataoutdir='])
except getopt.GetoptError, err:
    print str(err)
    usage()
    sys.exit(2)

verbose = False
html    = False
flao    = False
summary = None
logdir = '/aodata/UAO_logs'
wfslogdir=None
adseclogdir=None

day = None
side = None
dataoutdir = ''

for o,a in optlist:
    if o == '-v':
        verbose=True
    elif o in ('-h', '--help'):
        usage()
	sys.exit()
    elif o == '--day':
        day = a
    elif o == '--html':
        html = True
    elif o == '--summary':
        summary = a
    elif o == '--wfslogdir':
        wfslogdir = a
    elif o == '--adseclogdir':
        adseclogdir = a
    elif o == '--dataoutdir':
        dataoutdir = a
    elif o == '--side':
        side = a
    elif o == '--flao':
        flao = True


if day is None:
    print
    print 'Argument --day=YYYYMMDD is mandatory'
    print
    sys.exit(2)

if side is None:
    print
    print 'Argument --side=[L|R] is mandatory'
    print
    sys.exit(2)

if (side is not 'L') and (side is not 'R'):
    print
    print 'Argument --side must be L or R'
    print
    sys.exit(2)



def lookup_flao_logs(logdir, name, day):
    '''Returns a list of log files for the specified day'''

    start = calendar.timegm(time.strptime(day+' 000000', '%Y%m%d %H%M%S'))
    end   = calendar.timegm(time.strptime(day+' 235959', '%Y%m%d %H%M%S'))

    pattern = os.path.join(logdir, name)+'*log*'
    files = glob.glob(pattern)
    ret = []
    prev_file= ''
    done_prev = False
    done_next = False
    for file in sorted(files):
        parts = file.split('.')
        try:
            if parts[-1] == 'gz':
                timestamp = int(parts[-3])
            else:
                timestamp = int(parts[-2])
        except:
            continue

        if timestamp >= start and not done_prev:
            ret.append(prev_file)
            done_prev = True
            ret.append(file)
        elif timestamp >= start and timestamp <= end:
            if not done_prev:
                ret.append(prev_file)
                done_prev = True
            ret.append(file)
        elif timestamp > end:
            if not done_next:
                ret.append(file)
                done_next = True

        prev_file = file

    return filter(lambda x:x!='', ret)

def chain_logfiles(logfiles, grep=None):

   f = fileinput.input(files=logfiles, openhook=fileinput.hook_compressed)

   for line in f:
       if grep is not None and grep not in line:
           continue
       yield line


def logfilename(process, side, day, logdir=None, num=0):
    y = day[0:4]
    m = day[4:6]
    d = day[6:8]
    if process=='pyarg':
        path = '%s/%s/%s/%s.%s%04d.log' % (y, m, d, process, day, num)
    else:
        path = '%s/%s/%s/%s.%s.%s%04d.log' % (y, m, d, process, side, day, num)
    if logdir:
        path = os.path.join(logdir, path)
    return path
    

def log_timestamp(line):
    fields = line.split('|')
    timestamp, microsec = fields[3].split('.')
    return calendar.timegm( time.strptime(timestamp, '%Y-%m-%d %H:%M:%S')) + float(microsec)/1e6

def julianDayFromUnix(timestamp):
    return ( timestamp / 86400.0 ) + 2440587.5;

def timeStr(t):
    return time.strftime('%Y%m%d %H:%M:%S', time.gmtime(t))

def dayStr(t):
    return time.strftime('%Y%m%d', time.gmtime(t))

def hourStr(t):
    return time.strftime('%H:%M:%S', time.gmtime(t))


def flux2Mag(flux):
    refmag = 5.5
    refflux = 2970000.0
    return 2.5*math.log10(refflux / flux) + refmag

def logfile( name, side, day, logdir=None, grep=None):
    ''''
    Returns a file-like object to read a logfile
    '''

    if flao:
        logfiles = lookup_flao_logs(logdir, name, day)
        return chain_logfiles(logfiles, grep=grep)

    for n in range(10000):
        filename= logfilename( name, side, day, logdir=logdir, num=n)
        filenamegz = filename+'.gz'
        if os.path.exists(filename):
            if verbose:
                print 'Reading: '+filename
            if grep is not None:
                cmd = 'grep "%s" %s' % (grep, filename)
                return os.popen(cmd)
            else:
                return file(filename, 'r')

        if os.path.exists(filenamegz):
            if verbose:
                print 'Reading: '+filenamegz
            if grep is not None:
                cmd = 'gzip -cd %s | grep "%s"' % (filenamegz, grep)
                return os.popen(cmd)
            else:
                cmd = 'gzip -cd %s' % filenamegz
                return os.popen(cmd)
     
    raise Exception('Cannot find log file: '+filename)


def search( logdir, name, side, day, string=None, mindiff=1, getDict=False):

    found = logfile( name, side, day, logdir=logdir, grep=string)
    prev=0
    found2={}
    p = re.compile('\>  \. (.*)')
    for f in found:
        now = log_timestamp(f)
        if now-prev>= mindiff:
            if not found2.has_key(now):
	        found2[now] = f.strip()
            else:
                try:
                    fields = f.split('|')
                    m = p.search(fields[4])
                    if m:
                        found2[now] += m.group(1)
                except IndexError as e:
                    print 'Malformed line: '+f
        else:
            if verbose:
                print 'Rejected '+f.strip()
        prev=now

    if getDict:
        return found2

    found3 = []
    for k in sorted(found2.keys()):
        found3.append(found2[k])
    return found3

def myRound(x, ndigits=0):
    '''Returns a rounded number where -0.0 is set to 0'''
    y = round(x, ndigits)
    if y == 0.0:   # This yields True for both 0.0 and -0.0
       y = 0.0
    return y


class Event:
    def __init__(self, name, t, details=None):
        self.name = name
        self.t = t
        self.details = details

    def htmlHeader(self):
        return '<tr><th>Timestamp</th><th>Event</th><th>Details</th></tr>'

    def htmlRow(self):
        return '<tr><td>%s</td><td>%s</td><td>%s</td></tr>' % ( timeStr(self.t), self.name, self.details)



class SkipFrameEvent(Event):
    def __init__(self, t, details=''):
        Event.__init__( self, 'SkipFrame', t, details)

    @staticmethod
    def fromLogLine(line):
        t = log_timestamp(line)
        line = line.replace('->', '--') # Avoid extra ">"
        log, msg = line.split('>')
        return SkipFrameEvent( t, msg)

class FailedActuatorEvent(Event):
    def __init__(self, t, details, actno=None):
        Event.__init__( self, 'FailedActuator', t, details)
        self.actno = actno

    @staticmethod
    def fromLogLine(line):
        t = log_timestamp(line)
        pattern = 'Failing actuator detected N. (\d+)(.*)'
        found = re.findall( pattern, line)
        if len(found) > 0:
            actno, reason = found[0]
            log, msg = line.split('>')
        return FailedActuatorEvent( t, 'Act: %s - %s' % (actno, msg), int(actno))


class RIPEvent(Event):
    def __init__(self, t, details):
        Event.__init__( self, 'RIP', t, details)

    @staticmethod
    def fromLogLine(line):
        t = log_timestamp(line)
        try:
            procname, other = line.split('_')
            if procname=='fastdiagn':
                procname = 'FastDiagnostic'
            if procname=='housekeeper':
                procname = 'HouseKeeper'
            return RIPEvent( t, 'Detected by %s' % procname)
        except ValueError:
            return RIPEvent( t, '')

class SCTimeoutEvent(Event):
    def __init__(self, t, details):
        Event.__init__( self, 'Slope computer timeout', t, details)

    @staticmethod
    def fromLogLine(line):
        t = log_timestamp(line)
        return SCTimeout( t, '')



class ArbCmd:
    def __init__(self, name, args='', start_time=None, end_time=None, success=None, errstr=''):
        self.name = name
        self.args = args
        self.start_time = start_time
        self.end_time = end_time
        self.success = success
        self.errstr = errstr
        self.floatPattern ='[-+]?\d*\.\d+|d+'
        self.wfsPattern ='wfsSpec = (\w+)WFS'
        self.magPattern ='expectedStarMagnitude = (%s)' % self.floatPattern
        self.refXPattern = 'roCoordX = (%s)' % self.floatPattern
        self.refYPattern = 'roCoordY = (%s)' % self.floatPattern
        self.modePattern = 'mode = (\w+)'
 
    def report(self):
        timeStr = timeStr( self.start_time)
        if self.success is True:
            successStr = 'Success'
        elif self.success is False:
            successStr = 'Failure: %s' % self.errstr
        else:
            successStr = 'Unknown'

        return '%s %s' % (timeStr, successStr)
 
    def errorString(self):
        try:
            if self.name == 'PresetAO':

                str = self.errstr
                replaces = [('presetAO:', ''), 
                            ('WARNING -', ''),
                            ('RETRY:', ''),
                            ('(-20004) WFSARB_ARG_ERROR', ''),
                            ('(-5001) TIMEOUT_ERROR', ''),
                            ('(-5002) VALUE_OUT_OF_RANGE_ERROR', '')]

                for r in replaces:
                    str = str.replace(r[0], r[1])
                return str.strip()
        except:
            pass
        return self.errstr

    def is_instrument_preset(self):
        if not self.name == 'PresetAO':
            return False

        # Make sure mag, refX and refY are there
        _ = self.details()

        return self.refX != 0 or self.refY != 0 

    def details(self):

        details=[]
        try:
            if self.name == 'Offset' or self.name == 'OffsetXY':
                coords = map( float, re.findall( self.floatPattern, self.args))
                if len(coords) ==2:
                    return ['X=%.2f, Y=%.2f mm' % (coords[0], coords[1])]
                else:
                    return ['']

            if self.name == 'PresetAO':

                if flao:
                    self.wfs = 'FLAO'
                    self.mag = 8
                    self.refX = 0.1
                    self.refY = 0.1
                    return ['']

                wfs = re.findall( self.wfsPattern, self.args)[0]
                mag = float(re.findall( self.magPattern, self.args)[0])
                refX = float(re.findall( self.refXPattern, self.args)[0])
                refY = float(re.findall( self.refYPattern, self.args)[0])
                mode = re.findall(self.modePattern, self.args)[0]
                details.append('%s, star mag= %.1f, posXY= %.1f, %.1f mm, mode = %s' % (wfs, mag, myRound(refX, 1), myRound(refY, 1), mode))
                if hasattr(self, 'intervention'):
                    if self.intervention == True:
                        interventionDesc = 'Intervention mode'
                    else:
                        interventionDesc = 'Automatic mode'
                else:
                    interventionDesc = 'Intervention/automatic mode unknown'
              
                self.wfs = wfs
                self.mag = mag
                self.refX = refX
                self.refY = refY
                self.mode = mode
                details.append(interventionDesc) 

            if self.name == 'CompleteObs':
                tt = self.total_time()
                ot = self.open_time()
                if tt != 0:
                    per = ot*100/tt
                else:
                    per = 0
                s = '%s, open shutter: %ds (%d%%)' % (self.wfs, ot, per)
                details.append(s)

            if self.name == 'Acquire':
                details=[]
                if hasattr(self, 'estimatedMag'):
                    details.append('Estimated magnitude: %.1f' % self.estimatedMag)
                if hasattr(self, 'hoBinning'):
                    details.append('Ccd39 binning: %d' % self.hoBinning)
                if hasattr(self, 'hoSpeed'):
                    details.append('Loop speed: %d Hz' % self.hoSpeed)
        except Exception, e:
            print e
            pass
        return details


def search_arb_cmd( logdir, name, side, day, cmd, mindiff=1):

    lines = search( logdir, name, side, day, 'COMMANDHANDLER', mindiff=0)

    cmds=[]
    curCmd = False
    for line in lines:
        if curCmd is not None:
            curCmd.end_time = log_timestamp(line)
	    if "successfully completed" in line:
	        curCmd.success = True
	    if "Command execution failed" in line:
	        curCmd.success = False
		# Search for error cause
		cause = search( dir, name, cur_cmd, log_timestamp(line), 'error',mindiff=0)
		if len(cause)==0:
		    cause = search( dir, name, cur_cmd, log_timestamp(line), 'failed',mindiff=0)
		if len(cause)>0:
                    curCmd.errstr = cause[0][64:]
		else:
                    curCmd.errstr = 'Cannot detect reason'
	    if "Fsm is discarding" in line:
                curCmd.errstr = 'Rejected by FSM'
	    if "status RETRY" in line:
	        pos = line.find('errstr')
                curCmd.errstr = line[pos+7:-1]
            cmds.append(curCmd)
	    curCmd = False
	    continue

        if cmd in line:
            name = cmd
            args = None
            t = log_timestamp(line)
            curCmd = ArbCmd( name=name, args=args, start_time=t, end_time=None, success=None, errstr='')

    return cmds


def mix_lines(lines1, lines2):

     return sorted(lines1 + lines2, key=log_timestamp)

def get_AOARB_cmds( side, day):

    import re
    aoarb_lines = []
    pywfs_lines = []

    loggername = 'MAIN'
    if flao:
       loggername = 'COMMANDHANDLER'

    if adseclogdir:
       aoarb_lines = search( adseclogdir, 'AOARB', side, day, string=loggername, mindiff=0)
       # Use dummy wfs lines when running on the adsec
#       if not wfslogdir:
#          pywfs_lines= search( adseclogdir, 'pinger', side, day, 'MAIN', mindiff=0)

#    if wfslogdir:
#       pywfs_lines = search( wfslogdir, 'pyarg', side, day, 'MAIN', mindiff=0)
#


    lines = mix_lines(list(aoarb_lines), list(pywfs_lines))


    cmds=[]
    curCmd=None

    startCmdFlao = 'FSM (status'
    startCmdUao = 'Request:'

    p1 =re.compile('Request: (.*?)\((.*)\)')
    p2 =re.compile('Request: (.*)')
    p3 =re.compile('has received command \d+ \((.*)\)') # FLAO command

    endCmdFlao = ' successfully completed'
    endCmdUao  = 'Status after command:'

    exceptionStr  = '[AOException]'
    illegalCmdStr = 'Illegal command' 
    interventionStr = 'Intervention:'
    readyForStartStr = 'Status after command: AOArbitrator.ReadyForStartAO'
    estimatedMagStr = 'Estimated magnitude from ccd39: '
    hoBinningStr = 'HO binning  : '
    hoSpeedStr =   'HO speed    : '

    lastAcquireRef=None

    for line in lines:
      try:
        if (startCmdFlao in line) or (startCmdUao in line):

            # Skip this command, that has no effect on FSM
            if 'getLastImage' in line:
                continue

            if curCmd is not None:
                cmds.append(curCmd)
                curCmd = None

            t = log_timestamp(line)
            m1 = p1.search(line)
            m2 = p2.search(line)
            m3 = p3.search(line)
            name = None
            args = ''
            if m1:
                try:
                    name = m1.group(1)
                    args = m1.group(2)
                except IndexError as e:
                    print 'Malformed request: '+line
                    continue
            elif m2:
                try:
                    name = m2.group(1)
                except IndexError as e:
                    print 'Malformed request: '+line
                    continue
            elif m3:
                try:
                    name = m3.group(1)
                except IndexError as e:
                    print 'Malformed request: '+line
                    continue
            else:
                print 'Malformed request: '+line
                continue
            
 
            default_success = None
            if flao:
                default_success = False

            curCmd = ArbCmd( name=name, args=args, start_time=t, end_time=None, success=default_success, errstr='')
            if name == 'AcquireRefAO':
                lastAcquireRef = curCmd


        elif (endCmdFlao in line) or (endCmdUao in line):
            t = log_timestamp(line)
            curCmd.end_time = t
            curCmd.success = True
            curCmd.errstr = ''

            if readyForStartStr in line:
                if lastAcquireRef is not None:
                    lastAcquireRef.end_time = t
            continue

        elif exceptionStr in line:
            pos = line.index(exceptionStr)
            curCmd.errstr = line[pos+len(exceptionStr):].strip()
            curCmd.success = False

        elif illegalCmdStr in line:
            pos = line.index(illegalCmdStr)
            curCmd.errstr = line[pos:].strip()
            curCmd.success = False

        # Detect intervention mode in Presets
        elif interventionStr in line:
            pos = line.index(interventionStr) + len(interventionStr)
            interv = line[pos+1:pos+6]
            if interv[0:4] == 'True':
                curCmd.intervention=True
            else:
                curCmd.intervention=False

        # Detect magnitude estimation in AcquireRef 
        elif estimatedMagStr in line:
            pos = line.index(estimatedMagStr)
            curCmd.estimatedMag = float(line[pos+len(estimatedMagStr):])

        elif hoBinningStr in line:
            pos = line.index(hoBinningStr)
            curCmd.hoBinning = int(line[pos+len(hoBinningStr):])

        elif hoSpeedStr in line:
            pos = line.index(hoSpeedStr)
            curCmd.hoSpeed = int(line[pos+len(hoSpeedStr):].split()[0])

      except Exception, e:
        if verbose:
            print e
             
 
    # Store last command
    if curCmd is not None:
        cmds.append(curCmd)

    return cmds



class Interval:
    def __init__(self, start, end):
        self.start = start
        self.end = end

class CompleteObs(ArbCmd):

    def __init__(self, *args, **kwargs):
        ArbCmd.__init__(self, *args, **kwargs)
        self.cmds = []

    def total_time(self):
        '''Total observation time from start of PresetAO to end of StopAO'''
        if self.end_time is None or self.start_time is None:
            return 0
        return self.end_time - self.start_time

    def total_open_time(self):
        '''Total time available from instrument'''
        return self.total_time() - self.setup_duration() - self.offsets_overhead()

    def setup_duration(self):
        '''Total setup time from start of PresetAO to end of StartAO'''
        startao = filter(lambda x: x.name == 'StartAO' or x.name =='Start AO', self.cmds)[0]
        #print 'Setup duration:',  startao.end_time - self.start_time
        return startao.end_time - self.start_time

    def ao_setup_overhead(self):
        '''Total AO time from start of PresetAO to end of StartAO'''
        ao_time = 0
        is_intervention = False
        for cmd in self.cmds:
            if cmd.name in 'CenterStar CenterPupils CheckFlux CloseLoop'.split():
                #print 'INTERVENTION', cmd.name
                is_intervention = True

        for cmd in self.cmds:
           # print cmd.name, is_intervention, cmd.start_time, cmd.end_time
            if cmd.name in 'Acquire Done'.split():
                continue
            if cmd.end_time is None or cmd.start_time is None:
                continue
            if is_intervention and cmd.name == 'AcquireRefAO': # Avoid double counting acquisition commands
                continue

           # print timeStr(cmd.start_time), cmd.name, cmd.end_time - cmd.start_time

            if cmd.end_time is not None and cmd.start_time is not None:
                this_cmd_time = cmd.end_time - cmd.start_time
                ao_time += this_cmd_time
 #               print 'AO setup: ', cmd.name, this_cmd_time, ao_time
            if cmd.name == 'StartAO' or cmd.name == 'Start AO':
                return ao_time
        return 0

    def telescope_overhead(self):
        '''Telescope overhead during setup time'''
        return self.setup_duration() - self.ao_setup_overhead()

    def offsets_overhead(self):
        '''Time spent executing offsets'''
        offsets_time = 0
        for cmd in self.cmds:
            if cmd.name == 'PauseAO' or cmd.name == 'Pause':
                pause_time = cmd.start_time
            if cmd.name == 'ResumeAO' or cmd.name == 'Resume':
                resume_time = cmd.end_time
                offsets_time += resume_time - pause_time
        return offsets_time

    def total_ao_overhead(self):
        '''Time spent executing AO commands'''
        return self.ao_setup_overhead() + self.offsets_overhead()



def detectCompleteObs(cmds):
    '''
    Detect a complete observations series: PresetAO, Acquire,
    StarAO, and Stop.
    '''

    inObs = False
    inPreset = False
    newCmds = []
    for cmd in cmds:
        if cmd.name == 'PresetAO' and cmd.is_instrument_preset():
            inPreset = True
            inObs = False
            obsCmd = CompleteObs(name='CompleteObs', start_time = cmd.start_time)
            obsCmd.wfs = cmd.wfs
            obsCmd.mode = cmd.mode
            obsCmd.mag = cmd.mag
            obsCmd.cmds.append(cmd)

        elif inPreset is True and cmd.name == 'Cancel':
            inPreset = False
            inObs = False

        elif inObs is True:
            obsCmd.cmds.append(cmd)
            if cmd.name == 'Stop' or cmd.name == 'StopAO':
                obsCmd.end_time = cmd.end_time
                newCmds.append(obsCmd)
                inObs = False

        elif inPreset is True:
            obsCmd.cmds.append(cmd)
            if cmd.name == 'StartAO' or cmd.name == 'Start AO':
                inPreset = False
                inObs = True

        newCmds.append(cmd)

    return newCmds



def detectAcquires(cmds):
    '''
    Detect sequences of AcquireRefAO and subcommands and group them
    into a meta 'Acquire' command
    '''
    newCmds = []
    inAcquire = False
    for cmd in cmds:
        if cmd.name == 'AcquireRefAO':
            inAcquire = True
            acquireCmd = ArbCmd( name='Acquire', start_time=cmd.start_time)
            acquireCmd.success = cmd.success
            acquireCmd.errstr = cmd.errstr
            acquireDone = False

        elif inAcquire is True:

            if cmd.name == 'CheckFlux':
                acquireCmd.success = acquireCmd.success and cmd.success
                acquireCmd.errstr += cmd.errstr
                acquireCmd.end_time = cmd.end_time
                if hasattr(cmd, 'estimatedMag'):
                    acquireCmd.estimatedMag = cmd.estimatedMag
                if hasattr(cmd, 'hoBinning'):
                    acquireCmd.hoBinning = cmd.hoBinning
                if hasattr(cmd, 'hoSpeed'):
                    acquireCmd.hoSpeed = cmd.hoSpeed
                
            elif cmd.name == 'CenterPupils' or \
               cmd.name == 'CenterStar' or \
               cmd.name == 'CloseLoop' or \
               cmd.name == 'OptimizeGain' or \
               cmd.name == 'ReCloseLoop' or \
               cmd.name == 'getLastImage':
                acquireCmd.success = acquireCmd.success and cmd.success
                acquireCmd.errstr += cmd.errstr
                acquireCmd.end_time = cmd.end_time
 
            elif cmd.name == 'Done':
                acquireCmd.success = acquireCmd.success and cmd.success
                acquireCmd.errstr += cmd.errstr
                acquireCmd.end_time = cmd.end_time
                acquireDone = True

                if cmd.success==True:
                    newCmds.append(acquireCmd)
                    inAcquire=False

            else:
                if not acquireDone:
                    acquireCmd.success = False
                    acquireCmd.errstr += ' Command not completed'
                    newCmds.append( acquireCmd)
                    inAcquire = False

        newCmds.append(cmd)

    return newCmds
       

def detectOffsets(cmds):
    '''
    Detect Pause-Offset-Resume sequences and build a meta 'Offset' command
    for each of them.
    '''

    if len(cmds)<3:
        return cmds

    newCmds = []
    for n in range(len(cmds)-2):

        if cmds[n].name == 'Pause' and \
           cmds[n+1].name == 'OffsetXY' and \
           cmds[n+2].name == 'Resume':

           t0 = cmds[n+0].start_time
           t1 = cmds[n+2].end_time
           success = reduce( operator.and_, [x.success for x in cmds[n:n+3]])
           errstr = ' '.join([x.errstr for x in cmds[n:n+3]])
           args = cmds[n+1].args

           cmd = ArbCmd( name='Offset', args=args, start_time=t0, end_time=t1, success=success, errstr=errstr)
           newCmds.append(cmd) 

        elif cmds[n].name == 'Pause' and \
           cmds[n+1].name == 'OffsetXY':
           # Last command is not a Resume

           t0 = cmds[n+0].start_time
           t1 = cmds[n+1].end_time
           success = reduce( operator.and_, [x.success for x in cmds[n:n+2]])
           errstr = ' '.join([x.errstr for x in cmds[n:n+2]])
           args = cmds[n+1].args

           if success:
               success = False
               errstr = 'Resume was not sent'

           cmd = ArbCmd( name='Offset', args=args, start_time=t0, end_time=t1, success=success, errstr=errstr)
           newCmds.append(cmd) 


        elif cmds[n].name == 'Pause':
           # Next command is not an OffsetXY

           t0 = cmds[n].start_time
           t1 = cmds[n].end_time
           success = cmds[n].success
           errstr = cmds[n].errstr
           args = ''

           if success:
               success = False
               errstr = 'OffsetXY was not sent'

           cmd = ArbCmd( name='Offset', args=args, start_time=t0, end_time=t1, success=success, errstr=errstr)
           newCmds.append(cmd) 

        newCmds.append(cmds[n])

    return newCmds
        


def cmdsByName(cmds, name):
    return filter( lambda x: x.name == name, cmds)


def output(name, title, found, code=0):

    if not html:
        print
        print title

        print 'Total: %d' % len(found)
        for f in found:
             print f

    else:
	print '<HR>'
        print '<H2>%s</H2>' % title
	print '<p>Total: %d</p>' % len(found)
	if len(found)>0:
	    print '<p>'
        for f in found:
             print f+'<br>'
	if len(found)>0:
	    print '</p>'

    if dataoutdir != '':
       fileout = os.path.join(dataoutdir,'data')
       f = file(fileout,'a+')
       for line in found:
          f.write('%f %d\n' % (julianDayFromUnix(log_timestamp(line)), code))
       f.close()

def outputEvents( title, events, sort=True):

    if sort:
        ev = {}
        sortedEvents = []
        for e in events:
            ev[e.t] = e
        for k in sorted(ev.keys()):
            sortedEvents.append(ev[k])
    else:
        sortedEvents = events

    if not html:
        print
        print title

        print 'Total: %d' % len(sortedEvents)
        for e in sortedEvents:
             print '%s %s %s' % (timeStr(e.t), e.name, e.details)

    else:
	print '<HR>'
        print '<H2>%s</H2>' % title
	print '<p>Total: %d</p>' % len(sortedEvents)
	if len(sortedEvents)>0:
            print '<table id="aotable">'
            print sortedEvents[0].htmlHeader()
            for e in sortedEvents:
                print e.htmlRow()
            print '</table>'
        else:
            print '<p>'

def output(name, title, found, code=0):

    if not html:
        print
        print title

        print 'Total: %d' % len(found)
        for f in found:
             print f

    else:
	print '<HR>'
        print '<H2>%s</H2>' % title
	print '<p>Total: %d</p>' % len(found)
	if len(found)>0:
	    print '<p>'
        for f in found:
             print f+'<br>'
	if len(found)>0:
	    print '</p>'

    if dataoutdir != '':
       fileout = os.path.join(dataoutdir,'data')
       f = file(fileout,'a+')
       for line in found:
          f.write('%f %d\n' % (julianDayFromUnix(log_timestamp(line)), code))
       f.close()

def output_value(name, filename, found, pattern, func=None):

    values={}
    for f in found:
        values[f]=0
        v = re.search(pattern, f)
        if v:
            values[f] = float(v.groups()[0])
            if func != None:
                values[f] = func(values[f])

    if dataoutdir != '':
       fileout = os.path.join(dataoutdir, filename)
       f = file(fileout,'a+')
       for line in found:
          f.write('%f %f\n' % (julianDayFromUnix(log_timestamp(line)), values[line]))
       f.close()


def output_cmd( title, found, cmd_code='1'):

    success = len(filter(lambda x: x.success, found))
    success_rate = 0
    if len(found)>0:
        success_rate = float(success) / len(found)

    if not html:
        print
        print title

        print 'Total: %d - Success rate: %d%%' % (len(found), int(success_rate*100))
        for f in found:
             print f.report()

    else:
	print '<HR>'
        print '<H2>%s</H2>' % title
	print '<p>Total: %d - Success rate: %d%%</p>' % (len(found), int(success_rate*100))
	if len(found)>0:
	    print '<p>'
	    print '<table id="aotable">'
	    print '<tr><th>Time</th><th>Command</th><th>Ex. time (s)</th><th style="width: 300px">Result</th><th>Details</th></tr>'
        for cmd in found:
	     strtime = timeStr( cmd.start_time)
             if (cmd.end_time is not None) and (cmd.start_time is not None):
                 elapsed = '%5.1f s' % (cmd.end_time - cmd.start_time,)
             else:
                 elapsed = 'Unknown'
             if cmd.success is True:
                 errstr = 'Success'
             else:
                 errstr = cmd.errorString()
             print '<tr><td>%s</td><td>%s</td><td>%s</td><td style="width: 300px">%s</td><td>%s</td></tr>' % (strtime, cmd.name, elapsed, errstr, '<br>'.join(cmd.details()))
	if len(found)>0:
	    print '</table>\n'
	    print '</p>'

    if dataoutdir != '':
       fileout = os.path.join(dataoutdir,'data')
       f = file(fileout,'a+')
       for cmd in found:
          code = -cmd_code
          if cmd.success:
             code = cmd_code
          f.write('%f %d\n' % (julianDayFromUnix(k), code))
       f.close()

    return success_rate


def update_cmd_csv(cmds, day):

    csvfilename = os.path.join(dataoutdir, 'cmd_%s.csv' % side)
    # read csv
    if os.path.exists(csvfilename):
        with open(csvfilename, 'rb') as csvfile:
            data = list(csv.reader(csvfile, delimiter=','))
    else:
        data = []

    if len(cmds) < 1:
        return

    # Remove anything matching this day/cmd (assumes all cmds are equal)
    data = filter(lambda row: (row[0] != day) or (row[2] != cmds[0].name), data)

    # Remove header if any
    data = filter(lambda row: row[0] != 'day', data)

    # Add our data
    for cmd in cmds:
        if not cmd.success:
            continue
        if cmd.end_time is None or cmd.start_time is None:
            continue

        d = dayStr(cmd.start_time)
        h = hourStr(cmd.start_time)
        tottime = '%d' % (cmd.end_time - cmd.start_time)
        row = (d, h, cmd.name, tottime)
        data.append(row)

    data.sort(key=lambda x: x[0])

    hdr = ('day', 'hour', 'command', 'elapsed')
    data = [hdr]+data

    # Save csv   
    with open(csvfilename, 'wb') as csvfile:
        csv.writer(csvfile, delimiter=',').writerows(data)


def update_output_csv(cmds, day):

    csvfilename = os.path.join(dataoutdir, 'data_%s.csv' % side)

    # read csv
    if os.path.exists(csvfilename):
        with open(csvfilename, 'rb') as csvfile:
            data = list(csv.reader(csvfile, delimiter=','))
    else:
        data = []

    # Remove anything matching this day
    data = filter(lambda row: row[0] != day, data)

    # Remove header if any
    data = filter(lambda row: row[0] != 'day', data)

    # Add our data
    for cmd in cmds:
        d = dayStr(cmd.start_time)
        h = hourStr(cmd.start_time)
        tottime = '%d' % cmd.total_time()
        opentime = '%d' % cmd.total_open_time()
        setuptime = '%d' % cmd.setup_duration()
        aosetuptime = '%d' % cmd.ao_setup_overhead()
        telsetuptime = '%d' % cmd.telescope_overhead()
        offsetstime = '%d' % cmd.offsets_overhead()
        tottime_h = str(float(tottime)/3600)
        opentime_h = str(float(opentime)/3600)
         
        row = (d, h, tottime, tottime_h, opentime, opentime_h, setuptime, aosetuptime, telsetuptime, offsetstime, cmd.wfs, cmd.mode, cmd.mag)
        data.append(row)

    data.sort(key=lambda x: x[0])

    hdr = ('day', 'hour', 'time', 'time_h', 'open', 'open_h', 'setup', 'aosetup', 'telsetup', 'offsets', 'wfs', 'mode', 'magnitude')
    data = [hdr]+data

    # Save csv   
    with open(csvfilename, 'wb') as csvfile:
        csv.writer(csvfile, delimiter=',').writerows(data)


#########

table = {}
success = {}

if html:
    htmltitle = 'AO commands statistics for %s' % day
    print '''
<html>
<head>
  <title>%s</title>
  <link rel="stylesheet" href="aotable.css">
</head>
<body>
<H1>%s</H1>
''' % (htmltitle, htmltitle)


AOARB_cmds = get_AOARB_cmds( side, day)
AOARB_cmds = detectOffsets( AOARB_cmds)
AOARB_cmds = detectAcquires( AOARB_cmds)
AOARB_cmds = detectCompleteObs( AOARB_cmds)

update_output_csv(cmdsByName(AOARB_cmds, 'CompleteObs'), day)

update_cmd_csv(cmdsByName(AOARB_cmds, 'PresetAO'), day)
update_cmd_csv(cmdsByName(AOARB_cmds, 'CenterStar'), day)
update_cmd_csv(cmdsByName(AOARB_cmds, 'CenterPupils'), day)
update_cmd_csv(cmdsByName(AOARB_cmds, 'CheckFlux'), day)
update_cmd_csv(cmdsByName(AOARB_cmds, 'CloseLoop'), day)
update_cmd_csv(cmdsByName(AOARB_cmds, 'OptimizeGain'), day)
update_cmd_csv(cmdsByName(AOARB_cmds, 'ApplyOpticalGain'), day)
update_cmd_csv(cmdsByName(AOARB_cmds, 'OffsetXY'), day)

##########

string = 'CompleteObs'
title = 'Complete observations (from PresetAO to StopAO, instrument presets only)'


found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=1)
table['preset'] = len(found)
success['preset'] = success_rate

##########

string = 'PresetAO'
title = 'PresetAO'


found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=1)
table['preset'] = len(found)
success['preset'] = success_rate

##########

string = 'Acquire'
title  = 'Acquire - StartAO sequences'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=13)
table['acquire'] = len(found)
success['acquire'] = success_rate

##########

string = 'Offset'
title  = 'Pause - Offset - Resume sequences'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=15)
table['offset'] = len(found)
success['offset'] = success_rate


##########

string = 'AcquireRefAO'
title =  'AcquireRefAO'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=2)
table['acquireref'] = len(found)
success['acquireref'] = success_rate

##########

string = 'StartAO'
title  = 'StartAO'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=3)
table['startao'] = len(found)
success['startao'] = success_rate

##########

string = 'CenterStar'
title  = 'CenterStar'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=3)
table['centerstar'] = len(found)
success['centerstar'] = success_rate

##########

string = 'CenterPupils'
title  = 'CenterPupils'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=3)
table['centerpupils'] = len(found)
success['centerpupils'] = success_rate

##########

string = 'CheckFlux'
title  = 'CheckFlux'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=3)
table['checkflux'] = len(found)
success['checkflux'] = success_rate

##########

string = 'CloseLoop'
title  = 'CloseLoop'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=3)
table['closeloop'] = len(found)
success['closeloop'] = success_rate

##########

string = 'OptimizeGain'
title  = 'OptimizeGain'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=3)
table['optimizegain'] = len(found)
success['optimizegain'] = success_rate

##########

string = 'ApplyOpticalGain'
title  = 'ApplyOpticalGain'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=3)
table['applyopticalgain'] = len(found)
success['applyopticalgain'] = success_rate

##########

if wfslogdir:
    name = 'pyarg'
    string = 'Flux: '
    filename  = 'mag'

    found = search( wfslogdir, name, side, day, string)
    output_value( name, filename, found, 'Flux: (\d+)', flux2Mag)

##########

if wfslogdir:
    name = 'wfsarb'
    string = 'starMag ='
    filename  = 'presetmag'

    found = search( wfslogdir, name, side, day, string)
    output_value( name, filename, found, 'starMag = ([\d\.]+)')

##########
#
# Should be already included in RIPs detected
# by fastdiagnostic and housekeeper
#
#name = 'adsecarb'
#string = 'COILS DISABLED'
#title  = 'Shell RIP (any cause)'
#
#found = search( adseclogdir, name, side, day, string, mindiff=120)
#output( name, title, found, -7)
#table['totalRIP'] = len(found)
#
#########

name = 'M_FLAOWFS'
string = 'M_ADSEC: EXCD_BLOCK_ERROR'
title  = 'Peering stop msgd WFS -> ADSEC'

found=[]
#found = search( wfslogdir, name, side, day, string)
#output( name, title, found)
table['msgdBlock'] = len(found)


#########

name = 'M_ADSEC'
string = 'M_FLAOWFS: EXCD_BLOCK_ERROR'
title  = 'Peering stop msgd ADSEC -> WFS'

found=[]
#found = search( wfslogdir, name, side, day, string)
#output( name, title, found)
table['msgdBlock'] += len(found)

#########
#
# Events
#

events = []

name = 'AOARB'
string = ' - SkipFrame'

found = search( adseclogdir, name, side, day, string, mindiff=120)
events += map( SkipFrameEvent.fromLogLine, found)
table['skip'] = len(found)

#########

name = 'fastdiagn'
string = 'Failing actuator detected'

found = search( adseclogdir, name, side, day, string, mindiff=120)
events += map( FailedActuatorEvent.fromLogLine, found)
table['skip'] = len(found)

##########

name = 'fastdiagn'
string = 'FUNCTEMERGENCYST'

found = search( adseclogdir, name, side, day, string, mindiff=120)
events += map( RIPEvent.fromLogLine, found)
table['fastRIP'] = len(found)

########

name = 'housekeeper'
string = 'FUNCTEMERGENCYST'

found = search( adseclogdir, name, side, day, string, mindiff=120)
events += map( RIPEvent.fromLogLine, found)
table['housekeeperRIP'] = len(found)

##########

if wfslogdir:
    name = 'pyarg'
    string = 'AdOptError: Timeout waiting for slopecompctrl'

    found = search( wfslogdir, name, side, day, string)
    events += map( SCTimeoutEvent.fromLogLine, found)
    table['slopecomp'] = len(found)




title = 'Events'
outputEvents( title, events, sort=True)

##########

string = 'OffsetXY'
title  = 'OffsetXY'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=4)
table['offsetxy'] = len(found)
success['offsetxy'] = success_rate

##########

string = 'OffsetZ'
title  = 'OffsetZ'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=5)
table['offsetz'] = len(found)
success['offsetz'] = success_rate

##########

string = 'Pause'
title  = 'Pause'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=8)
table['loadshape'] = len(found)
success['loadshape'] = success_rate

##########

string = 'Resume'
title  = 'Resume'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=9)
table['loadshape'] = len(found)
success['loadshape'] = success_rate

##########

string = 'PowerOnAdSec'
title  = 'PowerOnAdsec'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=7)
table['loadshape'] = len(found)
success['loadshape'] = success_rate

##########

string = 'PresetFlat'
title  = 'PresetFlat'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=14)
table['loadshape'] = len(found)
success['loadshape'] = success_rate

##########

string = 'MirrorRest'
title  = 'MirrorRest'

found = cmdsByName( AOARB_cmds, string)
success_rate = output_cmd( title, found, cmd_code=15)
table['loadshape'] = len(found)
success['loadshape'] = success_rate

##########

if wfslogdir:

   name = 'pinger'
   string1 = 'Host ts8dx47: -1'
   string2 = 'Host ts8dx47: 3'
   title  = 'TS8 DX stops'

   found1 = search( wfslogdir, name, side, day, string1, mindiff=120, getDict=True)
   found2 = search( wfslogdir, name, side, day, string2, mindiff=120, getDict=True)
   v = {}
   for k in found1.keys():
       v[k] = 0
   for k in found2.keys():
       v[k] = 1
   found = []
   vv = 0
   off = None
   for k in v.keys():
       if vv==0 and v[k]==0:
           off = k
       if vv==0 and v[k]==1:
           found.append( v[k] + ' - %d seconds' % int(k-off))
       vv = v[k]
	
   output( name, title, found, -10)

#######

if html:
    print '''
</body>
</html>
'''


if summary:
    fsummary = file(summary+".txt", 'w')
    fsummary.write('Preset: %d - Acquire: %d - Cloop: %d - total RIPs: %d - msgd Block: %d' %
                   (table['preset'], table['acquireref'], table['startao'], table['totalRIP'], table['msgdBlock']))
    fsummary.close()

    fsummary = file(summary+".twiki", 'w')
    percent_preset = ''
    percent_acquireref = ''
    percent_startao = ''
    if table['preset']>0:
        percent_preset = '(%d%%)' % int(success['preset']*100)
    if table['acquireref']>0:
        percent_acquireref = '(%d%%)' % int(success['acquireref']*100)
    if table['startao']>0:
        percent_startao = '(%d%%)' % int(success['startao']*100)
       
    fsummary.write('| %d %s | %d %s | %d %s | %d | %d | %d |' %
                   (table['preset'], percent_preset, table['acquireref'], percent_acquireref, table['startao'], percent_startao, table['totalRIP'],
		   table['slopecomp'], table['msgdBlock']))
    fsummary.close()


