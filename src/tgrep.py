#! /usr/bin/python
'''
@author: Travis Patterson
@contact: (reddit) Massless
@contact: massless@unm.edu
@contact: 505-459-4730
@copyright: 2011
'''

import os
import sys
import re
from datetime import datetime, timedelta

class Tgrep():
    '''Search a log file for a range of lines between the given dates.'''
    DEFAULT_PATH = "/logs/haproxy.log"
    
    def __init__(self, pattern, file):
        '''
        Constructor
        takes:
            pattern - the time or timerange to search for
            file - the log file to search.
        '''
        #The log file being read
        self.__fd = os.open(file, os.O_RDONLY)
        #the size of the file, in bytes
        self.__file_size = os.fstat(self.__fd).st_size
        #list of lists containing (start pos, end pos) where the pattern matches 
        self.__ranges = []
        #A regular expression to pick timestamps out of text
        self.__search_expression = re.compile(r"\w{3}[ ]+\d+ \d{2}:\d{2}:\d{2}")
                
        #do the work
        start_time, end_time = self.__get_time_range(pattern)
        self.__find_range(start_time, end_time)
        self.__print_ranges()
        os.close(self.__fd)
                   
    def __get_time_range(self, pattern):
        '''
        Return the start and end times indicated by the pattern
        takes:
            pattern - the pattern defining the time range to search for.
        '''
        if not self.__check_pattern(pattern):
            print "invalid search pattern"
            sys.exit()
        
        if pattern.count('-'): #form HH:MM-HH:MM
            time_list = pattern.split('-')
            start = time_list[0] + ":00"
            end = time_list[1] + ":59"
        elif pattern.count(":") == 1: #form HH:MM
            start = pattern + ":00"
            end = pattern + ":59"
        else: #form HH:MM:SS
            start = pattern
            end = pattern
            
        return start, end
    
    def __check_pattern(self, pattern):
        '''
        verify that the provided search pattern is valid
        Valid patterns:
            HH:MM:SS
            HH:MM
            HH:MM-HH:MM
        '''
        form1 = r"\d\d:\d\d:\d\d"
        form2 = r"\d\d:\d\d"
        
        if pattern.count('-') == 1:
            times = pattern.split('-')
            time1 = re.match(form2, times[0]) 
            time2 = re.match(form2, times[1])
            return time1 is not None and time2 is not None        
        elif pattern.count(':') == 1:
            return re.match(form2,pattern) is not None 
        else: 
            return re.match(form1, pattern) is not None
        
    def __find_range(self, start, end):
        '''
        Find the file positions of the start and end times
        takes:
            start - start datetime object
            end - end datetime object
        '''
        one_day = timedelta(days=1)
                
        #convert start time and end time to datetime objects
        logdate = os.read(self.__fd, 6)
        start_date, end_date = self.__get_dates(logdate, start, end)
        
        #common case
        start = self.__binary_search(start_date, 0, self.__file_size, True)
        end = self.__binary_search(end_date, 0, self.__file_size, False)
        self.__ranges.append([start, end])
        
        #times may overlap at log roll, check tomorrow
        start = self.__binary_search(start_date + one_day, 0, self.__file_size, True)
        end = self.__binary_search(end_date + one_day, 0, self.__file_size, False)
        self.__ranges.append([start, end])
    
    def __get_dates(self, logdate, start_time, end_time):
        '''
        Return the start and end times as datetime objects
        takes:
            logdate - the month / day portion of timestamp in MMM DD format
            start_time - the start time in hh:mm:ss format
            end_time - the end time in hh:mm:ss
        '''
        one_day = timedelta(days=1)
        if len(logdate) < 6: #the file is malformed or empty, use today
            logdate = datetime.today().strftime("%b %d")
            
        st = self.__get_date("%s %s" % (logdate, start_time))
        et = self.__get_date("%s %s" % (logdate, end_time))
        
        if et < st: #If then end time is sooner than the start time, we've crossed midnight.
            et = et + one_day
        
        return st, et
    
    def __get_date(self, timestamp):
        '''
        Return a datetime object for the given timestamp
        takes:
            timestamp - timestamp from log in MMM DD HH:MM:SS format
        '''
        logdate = timestamp[:6]
        time = timestamp[7:]
        time_string = "%s %d %s" % (logdate, datetime.today().year, time)
        return datetime.strptime(time_string, "%b %d %Y %H:%M:%S")
       
    def __binary_search(self, time, left, right, start):
        '''
        perform a binary search on the file looking for a boundary between times
        takes:
            time - the time to find.
            left - the lower search bound in the file
            right - the upper search bound in the file
            start - whether or not we are looking for a start time.
                    acts as a hint to the binary search about which 
                    direction to search if both times are the same
                    determines which (first or second) position to
                    return in many cases.
        '''
        diff = (right - left) / 2
        curr = left + diff
        times = self.__get_times_at_position(curr)
        first_time = times["first"]["time"]
        second_time = times["second"]["time"]
        
        if not first_time or diff == 0:
            #The threshold either doens't exist or we're standing on it
            if first_time and start and first_time >= time:
                return times["first"]["position"]
            elif first_time and second_time and start:
                return times["second"]["position"]
            else:
                return -1
        elif first_time > time:
            return self.__binary_search(time, left, curr, start)
        elif second_time and second_time <= time:
            if second_time == time and first_time == time and start:
                return self.__binary_search(time, left, curr, start)
            else: 
                return self.__binary_search(time, curr, right, start)
        else: #first time <= time and second_time > time or none
            if start and first_time == time:
                # look for duplicates
                alt_pos = self.__binary_search(time, left, curr, start)
                if alt_pos != -1:
                    return alt_pos
                else:
                    return times["first"]["position"]
            elif second_time:
                return times["second"]["position"]
            elif start:
                return -1
            else:
                return self.__file_size
       
    def __get_times_at_position(self, position, size = 2048):
        '''
        read in size bytes from given position and
        find the first two timestamps in the string
        takes:
            position: the position to read from
            size: the number of bytes to read (default 2048)
        returns:
            timepositions: dictionary of the form -
                {name : {"timestamp : position in file"}}
                where name is first, second, third etc. 
        '''
        os.lseek(self.__fd, position, os.SEEK_SET)
        names = ["first", "second"]
        line = os.read(self.__fd, size)
        results = self.__search_expression.findall(line)
        positions = [self.__get_timeposition(results[i], position, line) \
                     for i in range(len(names)) if i < len(results)]
        num_positions = len(positions)
        positions += [{"time" : None, "position" : -1} \
                      for i in range(len(names) - num_positions)]
        timepositions = dict(zip(names, positions))
        
        if timepositions["first"]["time"] is None and position != 0:
            new_position = max(position - (size / 2), 0)
            return self.__get_times_at_position(new_position, size * 2)
        elif timepositions["second"]["time"] is None and position + size < self.__file_size:
            new_position = max(position - (size / 2), 0)
            return self.__get_times_at_position(new_position, size * 2)
        else:
            return timepositions
       
    def __get_timeposition(self, timestamp, read_position, read_line):
        '''Return a dict of the form {timestamp : datetime, position : position}'''
        ts = self.__get_date(timestamp)
        pos = read_position + read_line.index(timestamp)
        return {"time" : ts, "position" : pos}
    
    def __print_ranges(self):
        '''Write the contents of self.__ranges to stdout'''
        read_size = 81920
        for range in self.__ranges:
            if -1 not in range:
                os.lseek(self.__fd, range[0], os.SEEK_SET)
                count = range[1] - range[0]
                while count > 0:
                    to_read = min(count, read_size)
                    line = os.read(self.__fd, to_read)
                    sys.stdout.write(line)
                    count -= to_read


if __name__ == "__main__":
    #figure out the arguments to the class
    arg_len = len(sys.argv)
    if arg_len < 2 or arg_len > 3:
        print "Usage: tgrep <patten> [file] or tgrep [file] <pattern>"
        sys.exit()
    elif arg_len is 2:
        pattern = sys.argv[1]
        file = Tgrep.DEFAULT_PATH
    else:
        if os.path.exists(sys.argv[1]):
            pattern = sys.argv[2]
            file = sys.argv[1]
        elif os.path.exists(sys.argv[2]):
            pattern = sys.argv[1]
            file = sys.argv[2]
        else:
            print "Unable to open log file"
            sys.exit(-1)
    #Run it!
    tgrep = Tgrep(pattern,file)