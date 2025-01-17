# PyAlgoTrade
#
# Copyright 2011-2018 Gabriel Martin Becedillas Ruiz
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
.. moduleauthor:: Gabriel Martin Becedillas Ruiz <gabriel.becedillas@gmail.com>
"""

import abc

from pyalgotrade import bar
from pyalgotrade.dataseries import bards
from pyalgotrade import feed
from pyalgotrade import dispatchprio


# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency


class BaseBarFeed(feed.BaseFeed):
    """Base class for :class:`pyalgotrade.bar.Bar` providing feeds.

    :param frequency: The bars frequency. Valid values defined in :class:`pyalgotrade.bar.Frequency`.
    :param maxLen: The maximum number of values that the :class:`pyalgotrade.dataseries.bards.BarDataSeries` will hold.
        Once a bounded length is full, when new items are added, a corresponding number of items are discarded
        from the opposite end. If None then dataseries.DEFAULT_MAX_LEN is used.
    :type maxLen: int.

    .. note::
        This is a base class and should not be used directly.
    """

    def __init__(self, frequency, maxLen=None):
        super(BaseBarFeed, self).__init__(maxLen)
        if isinstance(frequency, list):
            self.__frequency = frequency
        else:
            self.__frequency = [frequency]
        self.__useAdjustedValues = False
        self.__defaultInstrument = None
        self.__currentBars = None
        self.__currentRealtimeBars = None
        self.__lastBars = {}

    def reset(self):
        self.__currentBars = None
        self.__currentRealtimeBars = None
        self.__lastBars = {}
        super(BaseBarFeed, self).reset()

    def setUseAdjustedValues(self, useAdjusted):
        if useAdjusted and not self.barsHaveAdjClose():
            raise Exception("The barfeed doesn't support adjusted close values")
        # This is to affect future dataseries when they get created.
        self.__useAdjustedValues = useAdjusted
        # Update existing dataseries
        for instrument in self.getRegisteredInstruments():
            self[instrument].setUseAdjustedValues(useAdjusted)

    # Return the datetime for the current bars.
    @abc.abstractmethod
    def getCurrentDateTime(self):
        raise NotImplementedError()

    # Return True if bars provided have adjusted close values.
    @abc.abstractmethod
    def barsHaveAdjClose(self):
        raise NotImplementedError()

    # Subclasses should implement this and return a pyalgotrade.bar.Bars or None if there are no bars.
    @abc.abstractmethod
    def getNextBars(self):
        """Override to return the next :class:`pyalgotrade.bar.Bars` in the feed or None if there are no bars.

        .. note::
            This is for BaseBarFeed subclasses and it should not be called directly.
        """
        raise NotImplementedError()

    def createDataSeries(self, key, maxLen):
        ret = bards.BarDataSeries(maxLen)
        ret.setUseAdjustedValues(self.__useAdjustedValues)
        return ret

    def getNextValues(self):
        dateTime = None
        bars = self.getNextBars()
        freq = bars.getBarFrequency()
        if bars is not None:
            dateTime = bars.getDateTime()

            # Check that current bar datetimes are greater than the previous one.
            if self.__currentBars is not None and self.__currentBars.getDateTime() > dateTime:
                if freq == self.__currentBars.getBarFrequency():
                    raise Exception(
                        "Bar date times are not in order. Previous datetime was %s and current datetime is %s" % (
                            self.__currentBars.getDateTime(),
                            dateTime
                        )
                    )

            # Update self.__currentBars and self.__lastBars
            self.__currentBars = bars
            for instrument in bars.getInstruments():
                self.__lastBars[instrument] = bars[instrument]
        return (dateTime, bars, freq)

    def getFrequency(self):
        return self.__frequency

    def isIntraday(self):
        for i in self.__frequency:
            if i < bar.Frequency.DAY:
                return True

    def getCurrentRealtimeBars(self):
        return self.__currentRealtimeBars

    def getCurrentBars(self):
        """Returns the current :class:`pyalgotrade.bar.Bars`."""
        return self.__currentBars

    def getLastBar(self, instrument):
        """Returns the last :class:`pyalgotrade.bar.Bar` for a given instrument, or None."""
        return self.__lastBars.get(instrument, None)

    def getDefaultInstrument(self):
        """Returns the last instrument registered."""
        return self.__defaultInstrument

    def getRegisteredInstruments(self):
        """Returns a list of registered intstrument names."""
        return self.getKeys()

    def registerInstrument(self, instrument, freq):
        self.__defaultInstrument = instrument
        self.registerDataSeries(instrument, freq)

    def getDataSeries(self, instrument=None, freq=None):
        """Returns the :class:`pyalgotrade.dataseries.bards.BarDataSeries` for a given instrument.

        :param instrument: Instrument identifier. If None, the default instrument is returned.
        :type instrument: string.
        :rtype: :class:`pyalgotrade.dataseries.bards.BarDataSeries`.
        """
        if instrument is None:
            instrument = self.__defaultInstrument
        return self[instrument, freq]

    def getDispatchPriority(self):
        return dispatchprio.BAR_FEED


class MultiFrequencyBarFeed(BaseBarFeed):
    def __init__(self, frequencies, maxLen=None):
        super(MultiFrequencyBarFeed, self).__init__(frequency=None, maxLen=maxLen)
        assert isinstance(frequencies, list)
        self.__frequencies = frequencies

    def getFrequency(self):
        raise Exception('{0} has multiple frequencies.'.format(self.__class__))

    def getFrequencies(self):
        return self.__frequencies


# This class is used by the optimizer module. The barfeed is already built on the server side,
# and the bars are sent back to workers.
class OptimizerBarFeed(BaseBarFeed):
    def __init__(self, frequency, instruments, bars, maxLen=None):
        super(OptimizerBarFeed, self).__init__(frequency, maxLen)
        for instrument in instruments:
            self.registerInstrument(instrument, frequency)
        self.__bars = bars
        self.__nextPos = 0
        self.__currDateTime = None

        try:
            self.__barsHaveAdjClose = self.__bars[0][instruments[0]].getAdjClose() is not None
        except Exception:
            self.__barsHaveAdjClose = False

    def getCurrentDateTime(self):
        return self.__currDateTime

    def barsHaveAdjClose(self):
        return self.__barsHaveAdjClose

    def start(self):
        super(OptimizerBarFeed, self).start()

    def stop(self):
        pass

    def join(self):
        pass

    def peekDateTime(self):
        ret = None
        if self.__nextPos < len(self.__bars):
            ret = self.__bars[self.__nextPos].getDateTime()
        return ret

    def getNextBars(self):
        ret = None
        if self.__nextPos < len(self.__bars):
            ret = self.__bars[self.__nextPos]
            self.__currDateTime = ret.getDateTime()
            self.__nextPos += 1
        return ret

    def eof(self):
        return self.__nextPos >= len(self.__bars)
