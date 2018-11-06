from pyalgotrade import bar
from pyalgotrade.dataseries import bards
from pyalgotrade import feed
from pyalgotrade import dispatchprio
from pyalgotrade.barfeed import BaseBarFeed
from quantlib.net2 import quote, history
import pyalgotrade.logger
from quantlib.const.market import TimeSeries
import pytz
import datetime

# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency

logger = pyalgotrade.logger.getLogger('livebarfeed')

class LiveBarFeed(BaseBarFeed):

    def __init__(self, instrument, frequency, maxLen=None,
                 start=datetime.datetime(1988, 1, 1)):
        super(LiveBarFeed, self).__init__(frequency, maxLen=maxLen)
        self.__instrument = instrument
        self.__last_date = None
        self.__bars_buf = []
        self.__data_downloaded = False
        self.__start_date = start

    def getCurrentDateTime(self):
        raise NotImplementedError()

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        if not self.__data_downloaded:
            logger.info('featch history of {0}'.format(self.__instrument))
            if self.getFrequency() == Frequency.DAY:
                tmp = history(self.__instrument, self.__start_date, None,
                              TimeSeries.TIME_DAILY)
            else:
                logger.error('unsupported frequency {0}'.format(self.getFrequency()))
            if tmp is None:
                return None
            for date, row in tmp.iloc[0].iterrows():
                tmpbar = bar.BasicBar(date, row['open'], row['high'],
                                      row['low'], row['close'], 0, False,
                                      self.getFrequency())
                self.__bars_buf.append(tmpbar)
            self.__data_downloaded = True
        try:
            tmp = self.__bars_buf.pop(0)
            return bar.Bars({self.__instrument: tmp})
        except:
            return None

    def getNextPeekBars(self):
        logger.info('get quote of {0}'.format(self.__instrument))
        tmp = quote(self.__instrument)
        if tmp is None:
            return None
        tmp = bar.BasicBar(datetime.datetime.utcnow().replace(tzinfo=pytz.utc),
                           tmp.iloc[0]['close'].iloc[0],
                           tmp.iloc[0]['close'].iloc[0],
                           tmp.iloc[0]['close'].iloc[0],
                           tmp.iloc[0]['close'].iloc[0],
                           0, False, self.getFrequency())
        ret = {self.__instrument: tmp}
        return bar.Bars(ret)

    def join(self):
        pass

    def eof(self):
        return False

    def peekDateTime(self):
        return None

    def start(self):
        super(LiveBarFeed, self).start()

    def stop(self):
        pass
