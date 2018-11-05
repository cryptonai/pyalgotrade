from pyalgotrade import bar
from pyalgotrade.dataseries import bards
from pyalgotrade import feed
from pyalgotrade import dispatchprio
from pyalgotrade.barfeed import BaseBarFeed
from quantlib.net2 import quote, history

import datetime

# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency


class LiveBarFeed(BaseBarFeed):

    def __init__(self, instrument, frequency, maxLen=None):
        super(LiveBarFeed, self).__init__(frequency, maxLen=maxLen)
        self.__instrument = instrument
        self.__last_date = None
        self.__bars_buf = []

    def getCurrentDateTime(self):
        raise NotImplementedError()

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        if not self.__bars_buf:
            tmp = history(self.__instrument)
            if tmp is None:
                return None
            for date, row in tmp.iloc[0]:
                tmpbar = bar.BasicBar(date, row['open'], row['high'],
                                      row['low'], row['close'], 0, False)
                self.__bars_buf.append(tmpbar)
        try:
            tmp = self.__bars_buf.pop(0)
            return bar.Bars({self.__instrument: tmp})
        except:
            return None

    def getNextPeekBars(self):
        tmp = quote(self.__instrument)
        if tmp is None:
            return None
        tmp = bar.BasicBar(datetime.datetime.utcnow(),
                           tmp.iloc[0]['close'],
                           tmp.iloc[0]['close'],
                           tmp.iloc[0]['close'],
                           tmp.iloc[0]['close'],
                           0, False)
        ret = {self.__instrument: tmp}
        return bar.Bars(ret)
