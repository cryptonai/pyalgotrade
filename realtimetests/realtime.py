from pyalgotrade import strategy
from pyalgotrade.barfeed import livefeed
from pyalgotrade import bar as barcls
import pyalgotrade.technical.atr as ptatr
import pyalgotrade.technical.ma as ptma
import datetime as dt
import talib
import numpy
import sys


class MyStrategy(strategy.LiveStrategy):
    def __init__(self, feed, instrument):
        super(MyStrategy, self).__init__(feed)
        self.__instrument = instrument

        ds = feed.getDataSeries(instrument=instrument, freq=barcls.Frequency.DAY)
        self.atr20 = ptatr.ATR(ds, period=20, maxLen=100)
        pds = feed[instrument, barcls.Frequency.DAY].getCloseDataSeries()
        self.ma10 = ptma.SMA(pds, period=10, maxLen=30)
        self.ma20 = ptma.SMA(pds, period=20, maxLen=60)
        self.ma58 = ptma.SMA(pds, period=58, maxLen=150)
        self.ma200 = ptma.SMA(pds, period=200, maxLen=250)

    def getATR(self):
        return self.atr20[-1]

    def onBars(self, bars):
        bar = bars[self.__instrument]

        if (self.ma10[-1] is not None and 
            self.ma20[-1] is not None and
            self.ma58[-1] is not None and 
            self.ma200[-1] is not None and
            self.atr20[-1] is not None):
            print('onBars {0} o:{1:.2f} h:{2:.2f} l:{3:.2f} c:{4:.2f} '
                'ma10:{5:.2f} ma20:{6:.2f} '
                'ma58:{7:.2f} ma200:{8:.2f} '
                'atr:{9:.2f} freq:{10}'.format(bar.getDateTime(),
                                bar.getOpen(),
                                bar.getHigh(),
                                bar.getLow(),
                                bar.getClose(),
                                self.ma10[-1], self.ma20[-1], self.ma58[-1], self.ma200[-1],
                                self.atr20[-1], bar.getFrequency()))

def main():
    # Load the bar feed from the CSV file
    feed = livefeed.LiveBarFeed('spot_gold@fx678', [barcls.Frequency.DAY,
                                                    barcls.Frequency.MINUTE,
                                                    barcls.Frequency.REALTIME],
                                start=dt.datetime(2017, 10, 1))
    #feed.addBarsFromCSV("orcl", "WIKI-ORCL-2000-quandl.csv")

    # Evaluate the strategy with the feed's bars.
    myStrategy = MyStrategy(feed, 'spot_gold@fx678')

    myStrategy.run()
    print("Final portfolio value: $%.2f" % myStrategy.getBroker().getEquity())

    print(type(myStrategy.getBroker()))


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    main()
