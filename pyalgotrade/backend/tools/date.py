import datetime as dt
from time import time
import pytz
import dateutil.parser as dp
import pandas as pd
from zipline.utils.calendars import get_calendar
import quantlib.tools.debug as dbg

def market_datetime_massage(start=None, end=None):
    '''
    process the start date and end date
    '''
    assert(start is None or type(start) == dt.datetime or
           type(start) == pd.Timestamp or type(start) == dt.date)
    assert(end is None or type(end) == dt.datetime or
           type(end) == pd.Timestamp or type(start) == dt.date)
    if not start:
        start = get_calendar('NYSE').all_sessions[0]
    if not end:
        end = pd.Timestamp.utcnow()
    if type(start) == pd.Timestamp:
        start = start.to_pydatetime()
    if type(end) == pd.Timestamp:
        end = end.to_pydatetime()
    if type(start) == dt.date:
        start = dt.datetime.combine(start, dt.time.min)
    if type(end) == dt.date:
        end = dt.datetime.combine(end, dt.time.min)
    return start, end


def date2timestamp(input):
    '''
    convert date to timestamp, input can be string, datetime, timestamp
    '''
    if isinstance(input, int) or isinstance(input, float):
        return input
    if isinstance(input, dt.datetime):
        return int(input.timestamp())
    if isinstance(input, str):
        parsed_t = dp.parse(input)
        return int(parsed_t.strftime('%s'))
    raise NotImplementedError('invalid date format %s'%str(input))


def dataframe_datetime_remove_time(df):
    '''
    remove dataframe index datetime time part but still keep the same type
    '''
    tmpdf = df.copy()
    dates = tmpdf.index.date
    tmpdf.index = dates
    tmpdf.index = pd.to_datetime(tmpdf.index, utc=True)
    tmpdf = tmpdf[~tmpdf.index.duplicated(keep='first')]
    return tmpdf


def utcnow_hourlevel():
    return dt.datetime.utcnow().replace(tzinfo=pytz.utc).replace(minute=0, second=0, microsecond=0)

def utcnow_daylevel():
    return dt.datetime.utcnow().replace(tzinfo=pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)
