#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import datetime as dt

import pandas as pd

import pytz
from pyalgotrade.bar import Frequency
from .mongocli import MongoHistoryData
from .query_layer import ql_history, parse_symbol_string

import logging

logger = logging.getLogger('pyalgotrade.livefeedbackend')

DBNAME = 'assets_history'


def cl_history(symbol_str, start=None, end=None,
                freq=Frequency.DAY, timezone='UTC',
                force_fetch=False):
    assert start is None or isinstance(start, dt.datetime) or isinstance(start, dt.date)
    assert end is None or isinstance(end, dt.datetime) or isinstance(end, dt.date)
    symbol, source = parse_symbol_string(symbol_str)
    if isinstance(start, dt.date):
        start = dt.datetime.combine(start, dt.datetime.min.time())
    if isinstance(end, dt.date):
        end = dt.datetime.combine(end, dt.datetime.min.time())
    hist = MongoHistoryData(DBNAME, 'collection_%s'%freq)
    first_date = hist.first_date(symbol, freq, source=source)
    last_date = hist.last_date(symbol, freq, source=source)
    # check start and end time
    if start is not None and start.tzinfo is None:
        start = start.replace(tzinfo=pytz.timezone(timezone))
        logger.debug('adding timezone %s to start time %s' % (timezone, start))
    elif start is None and first_date is not None:
        start = first_date
    else:
        start = dt.datetime(1983, 1, 1, tzinfo=pytz.timezone(timezone))
    if end is not None and end.tzinfo is None:
        end = end.replace(tzinfo=pytz.timezone(timezone))
        logger.debug('adding timezone %s to end time %s' % (timezone, end))
    elif end is None and last_date is not None:
        end = last_date
    else:
        end = dt.datetime.now(pytz.timezone(timezone))
    #now start and end is not empty
    # if first date or last date is empty, we need to download data
    if force_fetch:
        pn = ql_history(symbol_str, start, end, freq, add_missing_dates=False)
        if pn is not None:
            hist.save(pn.iloc[0], symbol, freq, source=source)
            return pn
        else:
            return None
    if not first_date or not last_date:
        pn = ql_history(symbol_str, start, end, freq, add_missing_dates=False)
        if pn is not None:
            hist.save(pn.iloc[0], symbol, freq, source=source)
            return pn
        else:
            return None

    if (first_date <= start and end <= last_date):
        # we can get data from database here
        df = hist.load(symbol, freq, start=start, end=end, source=source)
    elif (first_date > start and end > last_date):
        pn = ql_history(symbol_str, start, end, freq, add_missing_dates=False)
        if pn is not None:
            hist.save(pn.iloc[0], symbol, freq, source=source)
            return pn
        else:
            return None
    else:
        # we need to know what data to get from internet
        if end > last_date:
            pn = ql_history(symbol_str, last_date, end, freq, add_missing_dates=False)
            if pn is not None:
                hist.save(pn.iloc[0], symbol, freq, source=source)
            df = hist.load(symbol, freq, start=start, end=end, source=source)
        elif start < first_date:
            pn = ql_history(symbol_str, start, first_date, freq, add_missing_dates=False)
            if pn is not None:
                hist.save(pn.iloc[0], symbol, freq, source=source)
            df = hist.load(symbol, freq, start=start, end=end, source=source)
        else:
            raise Exception('start: %s end: %s first_date: %s last_date: %s' % (start, end, first_date, last_date))
    if df is not None and not df.empty:
        return pd.Panel({symbol_str: df})
    return None
