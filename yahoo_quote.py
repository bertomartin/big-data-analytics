from quote import Quote
import datetime
import urllib
import csv
import pdb

class YahooQuote(Quote):
    '''Daily quotes from Yahoo. Date format='yyyy-mm-dd' '''
    def __init__(self,symbol,start_date,end_date=datetime.date.today().isoformat()):
        super(YahooQuote,self).__init__()
        self.symbol = symbol.upper()
        start_year,start_month,start_day = start_date.split('-')
        start_month = str(int(start_month)-1) #expected by yahoo
        end_year,end_month,end_day = end_date.split('-')
        end_month = str(int(end_month)-1)
        url_string = "http://ichart.finance.yahoo.com/table.csv?s={0}".format(symbol)
        url_string += "&a={0}&b={1}&c={2}".format(start_month,start_day,start_year)
        url_string += "&d={0}&e={1}&f={2}".format(end_month,end_day,end_year)

        csv = urllib.urlopen(url_string).readlines()
        csv.reverse()
        for bar in xrange(0,len(csv)-1):
            ds,open_,high,low,close,volume,adjc = csv[bar].rstrip().split(',')
            open_,high,low,close,adjc = [float(x) for x in [open_,high,low,close,adjc]]
            if close != adjc:
                factor = adjc/close
                open_,high,low,close = [x*factor for x in [open_,high,low,close]]
            dt = datetime.datetime.strptime(ds,'%Y-%m-%d')
            self.append(dt,open_,high,low,close,volume)
        self.write_csv("./stocks/{0}.csv".format(symbol))

if __name__ == '__main__':
    with open('./finviz_tech.csv', 'rU') as csvfile:
        stockreader = csv.reader(csvfile)
        for row in stockreader:
            YahooQuote(row[0], '2000-1-1', '2014-12-31')
    # aapl = YahooQuote('ADBE', '2009-1-1')
