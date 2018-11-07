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
import time

# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency

logger = pyalgotrade.logger.getLogger('livebarfeed')

class LiveBarFeed(BaseBarFeed):

    def __init__(self, instrument, frequency, maxLen=None,
                 start=datetime.datetime(1988, 1, 1), sleep=60):
        super(LiveBarFeed, self).__init__(frequency, maxLen=maxLen)
        self.__instrument = instrument
        self.__last_date = None
        self.__bars_buf = []
        self.__data_downloaded = False
        self.__start_date = start
        self.__sleeptime = sleep

        self.__nextRealtimeBars = {
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
            if self.getFrequency() == Frequency.DAY:
                tmp = history(self.__instrument, self.__start_date, None,
                              TimeSeries.TIME_DAILY, add_missing_dates=False)
            elif self.getFrequency() == Frequency.HOUR:
                tmp = history(self.__instrument, self.__start_date, None,
                              TimeSeries.TIME_60MIN, add_missing_dates=False)
            else:
                logger.error('unsupported frequency {0}'.format(self.getFrequency()))
                assert False
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

    def getNextRealtimeBars(self):
        if self.__sleeptime > 0:
            time.sleep(self.__sleeptime)
        logger.info('get quote of {0}'.format(self.__instrument))
        tmp = quote(self.__instrument)
        if tmp is None:
            return None
        tmpVal = tmp.iloc[0]['close'].iloc[0]
        curTime = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        tmp = bar.BasicBar(curTime,
                           tmpVal, tmpVal, tmpVal, tmpVal,
                           0, False, self.getFrequency())
        if self.__nextRealtimeBars['open'] is None:
            self.__nextRealtimeBars['open'] = tmpVal
            self.__nextRealtimeBars['high'] = tmpVal
            self.__nextRealtimeBars['low'] = tmpVal
            self.__nextRealtimeBars['close'] = tmpVal
            if self.getFrequency() == Frequency.DAY:
                curTime = curTime.replace(hour=0, minute=0, second=0, microsecond=0)
            elif self.getFrequency() == Frequency.HOUR:
                curTime = curTime.replace(minute=0, second=0, microsecond=0)
            else:
                logger.error('{0} is not supported.'.format(self.getFrequency()))
                assert False
            self.__nextRealtimeBars['start'] = curTime
        else:
            if tmpVal > self.__nextRealtimeBars['high']:
                self.__nextRealtimeBars['high'] = tmpVal
            elif tmpVal < self.__nextRealtimeBars['low']:
                self.__nextRealtimeBars['low'] = tmpVal
            deltaTime = curTime - self.__nextRealtimeBars['start']
            if self.getFrequency() == Frequency.DAY and deltaTime.total_seconds() > 60*60*24:
                self.__nextRealtimeBars['close'] = tmpVal
                row = self.__nextRealtimeBars
                tmpbar = bar.BasicBar(curTime, row['open'], row['high'],
                                      row['low'], row['close'], 0, False,
                                      self.getFrequency())
                self.__bars_buf.append(tmpbar)
                self.__nextRealtimeBars = {
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'start' : None,
                }
                return None
            elif self.getFrequency() == Frequency.HOUR and deltaTime.total_seconds() > 60*60:
                self.__nextRealtimeBars['close'] = tmpVal
                row = self.__nextRealtimeBars
                tmpbar = bar.BasicBar(curTime, row['open'], row['high'],
                                      row['low'], row['close'], 0, False,
                                      self.getFrequency())
                self.__bars_buf.append(tmpbar)
                self.__nextRealtimeBars = {
                    'open': None,
                    'high': None,
                    'low': None,
                    'close': None,
                    'start' : None,
                }
                return None
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
