from pyalgotrade import bar
from pyalgotrade.dataseries import bards
from pyalgotrade import feed
from pyalgotrade import dispatchprio
from pyalgotrade.barfeed import BaseBarFeed, MultiFrequencyBarFeed
from quantlib.net2 import quote, history
import pyalgotrade.logger
from quantlib.const.market import TimeSeries
import pytz
import datetime
import time

# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency

logger = pyalgotrade.logger.getLogger('livebarfeed')

class LiveBarFeed(MultiFrequencyBarFeed):

    def __init__(self, instrument, frequencies, maxLen=1000,
                 start=datetime.datetime(1988, 1, 1), pullDelay=30):
        if not isinstance(frequencies, list):
            frequencies = [frequencies]
        super(LiveBarFeed, self).__init__(frequencies, maxLen=maxLen)

        # proactivly register our instrument just incase not found if
        # someone tries to call getDataSeries()
        for i in frequencies:
            self.registerDataSeries(instrument, i)

        self.__instrument = instrument
        self.__last_date = None
        self.__bars_buf = []
        self.__data_downloaded = False
        self.__start_date = start
        self.__pullDelay = pullDelay
        self.__nextRealtimeBars = {}
        self.__isRealTime = Frequency.REALTIME in frequencies
        for i in frequencies:
            if self.__isRealTime and i != Frequency.REALTIME and pullDelay > i:
                    logger.error('pull delay is larger than minimum frequency.')
                    assert False
            self.__nextRealtimeBars[i] = {
                'open': None,
                'high': None,
                'low': None,
                'close': None,
                'start' : None,
            }

    def getCurrentDateTime(self):
        raise NotImplementedError()

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        if not self.__data_downloaded:
            logger.info('featch history of {0}'.format(self.__instrument))
            for i in self.getFrequencies():
                tmp = None
                if i == Frequency.DAY:
                    tmp = history(self.__instrument, self.__start_date, None,
                                TimeSeries.TIME_DAILY, add_missing_dates=False)
                elif i == Frequency.HOUR:
                    tmp = history(self.__instrument, self.__start_date, None,
                                TimeSeries.TIME_60MIN, add_missing_dates=False)
                if tmp is None:
                    continue
                for date, row in tmp.iloc[0].iterrows():
                    tmpbar = bar.BasicBar(date, row['open'], row['high'],
                                        row['low'], row['close'], 0, False,
                                        i)
                    self.__bars_buf.append(tmpbar)
            self.__data_downloaded = True
            self.__bars_buf.sort(key=lambda i: i.getDateTime())
        while True:
            if self.__bars_buf:
                tmp = self.__bars_buf.pop(0)
                return bar.Bars({self.__instrument: tmp}, frequecy=tmp.getFrequency())
            else:
                self.generateBars()

    def generateBars(self):
        if self.__pullDelay > 0:
            time.sleep(self.__pullDelay)
        logger.info('get quote of {0}'.format(self.__instrument))
        tmp = quote(self.__instrument)
        if tmp is None:
            logger.error('failed to get {0} quote'.format(self.__instrument))
            return
        tmpVal = tmp.iloc[0]['close'].iloc[0]
        curTime = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        tmp = bar.BasicBar(curTime,
                           tmpVal, tmpVal, tmpVal, tmpVal,
                           0, False, Frequency.REALTIME)
        for freq in self.__nextRealtimeBars.keys():
            if self.__nextRealtimeBars[freq]['open'] is None:
                self.__nextRealtimeBars[freq]['open'] = tmpVal
                self.__nextRealtimeBars[freq]['high'] = tmpVal
                self.__nextRealtimeBars[freq]['low'] = tmpVal
                self.__nextRealtimeBars[freq]['close'] = tmpVal
                if freq == Frequency.DAY:
                    curTime = curTime.replace(hour=0, minute=0, second=0, microsecond=0)
                elif freq == Frequency.HOUR:
                    curTime = curTime.replace(minute=0, second=0, microsecond=0)
                elif freq == Frequency.MINUTE:
                    curTime = curTime.replace(second=0, microsecond=0)
                elif freq != Frequency.REALTIME:
                    logger.error('{0} is not supported.'.format(freq))
                    assert False
                self.__nextRealtimeBars[freq]['start'] = curTime
            else:
                if tmpVal > self.__nextRealtimeBars[freq]['high']:
                    self.__nextRealtimeBars[freq]['high'] = tmpVal
                elif tmpVal < self.__nextRealtimeBars[freq]['low']:
                    self.__nextRealtimeBars[freq]['low'] = tmpVal
                deltaTime = curTime - self.__nextRealtimeBars[freq]['start']
                if (Frequency.MINUTE == freq and deltaTime.total_seconds() > 60 or
                        Frequency.DAY == freq and deltaTime.total_seconds() > 60*60*24 or
                        Frequency.HOUR == freq and deltaTime.total_seconds() > 60*60):
                    self.__nextRealtimeBars[freq]['close'] = tmpVal
                    row = self.__nextRealtimeBars[freq]
                    tmpbar = bar.BasicBar(curTime, row['open'], row['high'],
                                        row['low'], row['close'], 0, False,
                                        freq)
                    self.__bars_buf.append(tmpbar)
                    self.__nextRealtimeBars[freq] = {
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'start' : None,
                    }
                    self.__bars_buf.append(tmp)
        if self.__isRealTime:
            self.__bars_buf.append(tmp)
        self.__bars_buf.sort(key=lambda i: i.getDateTime())

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
