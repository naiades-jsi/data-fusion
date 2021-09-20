import datetime

class TimeParser():
    	
    def __init__(self):
        pass

    def parseToInt(self, time):
        if isinstance(time, int):
            int_time = int(time)
        elif (time[-2:-1] == 'ms'):
            int_time = int(time[0:-2])
        elif (time[-1] == 's'):
            int_time = int(time[0:-1]) * 1000
        elif (time[-1] == 'm'):
            int_time = int(time[0:-1]) * 1000 * 60
        elif (time[-1] == 'h'):
            int_time = int(time[0:-1]) * 1000 * 60 * 60
        elif (time[-1] == 'd'):
            int_time = int(time[0:-1]) * 1000 * 60 * 60 * 24
        elif (time[-1] == 'w'):
            int_time = int(time[0:-1]) * 1000 * 60 * 60 * 24 * 7
        else:
            print(f"Wrong time pased: {time}")

        return int_time

    def parseToMS(self, time):
        ms_time = str(self.parseToInt(time)) + 'ms'

        return ms_time

    def parseDate(self, date):
        unix_date = ((datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S") - datetime.datetime(1970, 1, 1)).total_seconds())

        return int(unix_date) * 1000

    def diffFromNow(self, date):
        unix_diff = (datetime.datetime.now() - (datetime.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S"))).total_seconds()
        
        return (int(unix_diff) * 1000)
