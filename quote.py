class Quote(object):
    DATE_FMT = '%Y-%m-%d'
    TIME_FMT = '%H:%M:%S'

    def __init__(self):
        self.symbol = ''
        self.date, self.time, self.open_, self.high, self.low, self.close, self.volume = ([] for _ in range(7))

    def append(self, dt, open_, high, low, close, volume):
        self.date.append(dt.date())
        self.time.append(dt.time())
        self.open_.append(float(open_))
        self.high.append(float(high))
        self.low.append(float(low))
        self.close.append(float(close))
        self.volume.append(int(volume))

    def to_csv(self):
        return ''.join(["{0},{1},{2},{3:.2f},{4:.2f},{5:.2f},{6:.2f},{7}\n".format(self.symbol,
        self.date[bar].strftime('%Y-%m-%d'),self.time[bar].strftime('%H:%M:%S'),
        self.open_[bar], self.high[bar], self.low[bar], self.close[bar], self.volume[bar])
        for bar in xrange(len(self.close))])

    def write_csv(self, filename):
        with open(filename, 'w') as f:
            f.write("ticker,date,time,open,high,low,close,volume\n")
            f.write(self.to_csv())

    def read_csv(self, filename):
        self.symbol = ''
        self.date, self.time, self.open_, self.high, self.low, self.close, self.volume = ([] for _ in range(7))
        for line in open(filename, 'r'):
            symbol,ds,ts,open_,high,low,close,volume = line.rstrip().split(',')
            self.symbol = symbol
            dt = datetime.datetime.strptime(ds+' '+ts, self.DATE_FMT+' '+self.TIME_FMT)
            self.append(dt, open_, high, low, close, volume)
        return True

    def __repr__(self):
        return self.to_csv()
