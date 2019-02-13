from pyalgotrade import strategy
from pyalgotrade.barfeed import livefeed
from pyalgotrade import bar
import pyalgotrade.technical.atr as ptatr
from quantlib.const.market import TimeSeries
import datetime as dt
import talib
import numpy

class MovingAverage(object):
    def __init__(self, length):
        self.data = []
        self.length = length
    
    def pushData(self, newData):
        assert len(self.data) <= self.length
        if len(self.data) == self.length:
            self.data.pop(0)
        self.data.append(newData)

    def getRawData(self):
        return self.data

    def currentValue(self):
        if self.length != len(self.data):
            return numpy.nan
        else:
            return sum(self.data) / float(len(self.data))


class ATR(object):
    def __init__(self, length):
        self.__high = []
        self.__low = []
        self.__close = []
        self.length = length

    def pushData(self, newData):
        assert len(self.__high) <= self.length
        if len(self.__high) == self.length:
            self.__high.pop(0)
            self.__low.pop(0)
            self.__close.pop(0)
        self.__high.append(newData['high'])
        self.__low.append(newData['low'])
        self.__close.append(newData['close'])

    def currentValue(self):
        assert len(self.__high) <= self.length
        if len(self.__high) != self.length :
            return numpy.nan
        else:
            high = numpy.array(self.__high, dtype=float)
            low = numpy.array(self.__low, dtype=float)
            close = numpy.array(self.__close, dtype=float)
            #print(high, low, close)
            tmp = talib.ATR(high, low, close, timeperiod=self.length-1)
            #print(tmp)
            return tmp[-1]

class MyStrategy(strategy.LiveStrategy):
    def __init__(self, feed, instrument):
        super(MyStrategy, self).__init__(feed)
        self.__instrument = instrument
        
        self.atr20 = ATR(20)
        ma10 = MovingAverage(10)
        ma20 = MovingAverage(20)
        ma58 = MovingAverage(58)
        ma200 = MovingAverage(200)
        self.__ma = [ma10, ma20, ma58, ma200]

    def getMA(self, days):
        for i in self.__ma:
            if i.length == days:
                return i.currentValue()
        raise Exception('no such MA({0})'.format(days))

    def getATR(self):
        return self.atr20.currentValue()

    def calculateData(self, bar):
        self.atr20.pushData({'open': bar.getOpen(),
                     'high': bar.getHigh(),
                     'low': bar.getLow(),
                     'close': bar.getClose()})
        for i in self.__ma:
            i.pushData(bar.getClose())

    def onBars(self, bars):
        bar = bars[self.__instrument]
        self.calculateData(bar)
        
        ma10 = self.getMA(10)
        ma20 = self.getMA(20)
        ma58 = self.getMA(58)
        ma200 = self.getMA(200)
        atr = self.getATR()
        print('onBars {0} o:{1:.2f} h:{2:.2f} l:{3:.2f} c:{4:.2f} '
              'ma10:{5:.2f} ma20:{6:.2f} '
              'ma58:{7:.2f} ma200:{8:.2f} '
              'atr:{9:.2f} freq:{10}'.format(bar.getDateTime(),
                              bar.getOpen(),
                              bar.getHigh(),
                              bar.getLow(),
                              bar.getClose(),
                              ma10, ma20, ma58, ma200,
                              atr, bar.getFrequency()))

def test1():
    # Load the bar feed from the CSV file
    feed = livefeed.LiveBarFeed('spot_gold@fx678', [bar.Frequency.DAY,
                                                    bar.Frequency.MINUTE,
                                                    bar.Frequency.REALTIME],
                                start=dt.datetime(2018, 10, 1))
    #feed.addBarsFromCSV("orcl", "WIKI-ORCL-2000-quandl.csv")

    # Evaluate the strategy with the feed's bars.
    myStrategy = MyStrategy(feed, 'spot_gold@fx678')

    ds = feed.getDataSeries(instrument='spot_gold@fx678', freq=bar.Frequency.DAY)
    atr = ptatr.ATR(ds, period=20, maxLen=100)

    myStrategy.run()
    print("Final portfolio value: $%.2f" % myStrategy.getBroker().getEquity())

    print(type(myStrategy.getBroker()))

def test2():
    obj = MovingAverage(10)
    for i in range(0, 100):
        obj.pushData(i)
        print('{0}, MA {1}'.format(obj.getRawData(), obj.currentValue()))

test1()
