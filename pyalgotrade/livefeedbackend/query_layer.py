#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file is for get quote and history data
"""
import logging
import time
import os
import sys
import pkgutil
import importlib

from abc import ABCMeta, abstractmethod
import datetime as dt
import json
import six
import hashlib

import pandas as pd
import pytz
from zipline.utils.calendars import get_calendar
from pyalgotrade.bar import Frequency


logger = logging.getLogger('pyalgotrade.livefeedbackend')


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


def get_provider_name(cls):
    name = cls.__class__.__name__
    rtn = name.lower().replace('provider', '')
    return rtn


def parse_symbol_string(symbol_str):
    tmp = symbol_str.count('@')
    if tmp == 1:
        rtn = symbol_str.split('@')
        return rtn[0], rtn[1]
    elif tmp == 0:
        return symbol_str, None
    else:
        return None, None


class MarketDataProvider(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self._support_symbols = {}

    def add_support_symbol(self, symbol, data):
        self._support_symbols[symbol] = data

    def get_support_symbol(self, symbol):
        if symbol in self._support_symbols:
            return self._support_symbols[symbol]
        return None
    def has_support_symbol(self, symbol):
        if symbol in self._support_symbols:
            return True
        return False

    @abstractmethod
    def quote(self, symbol):
        pass

    @abstractmethod
    def history(self, symbol):
        pass

    @abstractmethod
    def search_symbol(self, value):
        pass



def register_agent(agt_cls):
    """
    register an agent
    """
    assert issubclass(agt_cls, MarketDataProvider)
    logger.debug('registering provider class %s' % agt_cls.__name__)
    if not hasattr(MarketDataAgent.provider_instances_list, agt_cls.__name__):
        MarketDataAgent.provider_instances_list[agt_cls.__name__] = agt_cls()
    return agt_cls


class MarketDataAgent(object):
    """
    Base agent class for downloading stock data
    """
    provider_instances_list = {}
    quote_cache = {}
    fundamental_cache = {}
    history_cache = {}

    def __init__(self, *args, **kwargs):
        raise Exception(
            'initializing class %s instance is not allowed' %
            self.__class__.__name__)

    @classmethod
    def get_quote(cls, symbol, source=None):
        ''' TODO: source is not used here right now
        '''
        rtn = None
        if symbol in MarketDataAgent.quote_cache and source is None:
            try:
                logger.debug('using cached provider match')
                rtn = MarketDataAgent.quote_cache[symbol].quote(symbol)
                if not isinstance(rtn, type(None)):
                    return rtn
                else:
                    MarketDataAgent.quote_cache.pop(symbol)
            except Exception as e:
                MarketDataAgent.quote_cache.pop(symbol)
                logger.warning('cache quote exception %s' % str(e))
        for name, v in cls.provider_instances_list.items():
            try:
                if source: 
                    if name.lower().find(source.lower()) != -1:
                        logger.debug('trying provider %s for symbol %s' %
                                     (v.__class__.__name__, symbol))
                        rtn = v.quote(symbol)
                    else:
                        continue
                else:
                    rtn = v.quote(symbol)
                if isinstance(rtn, type(None)):
                    logger.debug('%s does not support symbol %s!' %
                                (v.__class__.__name__, symbol))
                    continue
                MarketDataAgent.quote_cache[symbol] = v
                return rtn
            except Exception as e:
                logger.debug(str(e))
        logger.error('cannot find a quote provider')
        return None

    @classmethod
    def get_history(cls, symbol, start, end, freq, source=None):
        rtn = None
        if symbol in MarketDataAgent.history_cache and source is None:
            try:
                logger.debug('using cached provider match')
                rtn = MarketDataAgent.history_cache[symbol].history(symbol,
                                                                    start,
                                                                    end,
                                                                    freq)
                if not isinstance(rtn, type(None)):
                    return rtn
                else:
                    MarketDataAgent.history_cache.pop(symbol)
            except Exception as e:
                MarketDataAgent.history_cache.pop(symbol)
                logger.warning('cache history exception %s' % str(e))
        for name, v in cls.provider_instances_list.items():
            try:
                if source: 
                    if name.lower().find(source.lower()) != -1:
                        rtn = v.history(symbol, start, end, freq)
                    else:
                        continue
                else:
                    logger.debug('trying %s...' % name)
                    rtn = v.history(symbol, start, end, freq)
                if rtn is None:
                    continue
                MarketDataAgent.history_cache[symbol] = v
                return rtn
            except Exception as e:
                logger.debug('exception in {0}: {1} {2}'.format(name, type(e), str(e)))
        logger.error('cannot find a history provider')
        return None


    @classmethod
    def search_symbol(cls, value):
        for _, v in cls.provider_instances_list.items():
            try:
                rtn = v.search_symbol(value)
                if isinstance(rtn, type(None)):
                    continue
                return rtn
            except Exception:
                continue
        logger.error('cannot find a symbol')
        return None



def ql_quote(symbol_in):
    ''' get stock quote
    '''
    if isinstance(symbol_in, list):
        logger.warning('quote symbol is a list %s' % symbol_in)
        rtn = []
        for i in symbol_in:
            symbol, source = parse_symbol_string(i)
            tmp = MarketDataAgent.get_quote(symbol, source)
            if not isinstance(tmp, type(None)):
                rtn.append(tmp)
        return rtn
    elif isinstance(symbol_in, str):
        symbol, source = parse_symbol_string(symbol_in)
        return MarketDataAgent.get_quote(symbol, source)
    else:
        logger.error('quote symbol error')
        return None


def ql_history(symbol_str, start=None, end=None,
            freq=Frequency.DAY,
            add_missing_dates=True, calendar_days=None,
            benchmark='SPY'):
    ''' get stock history data
    '''
    logger.debug('start: %s end: %s' % (start, end))
    symbol, source = parse_symbol_string(symbol_str)
    if symbol is None:
        return None
    start, end = market_datetime_massage(start, end)
    rtn = MarketDataAgent.get_history(symbol, start, end, freq,
                                      source=source)
    if rtn is None:
        logger.error('failed to find %s data' % symbol)
        return None
    rtn = rtn.iloc[0]
    if add_missing_dates:
        if calendar_days is None:
            tmp = MarketDataAgent.get_history(benchmark, start, end, freq,
                                              source='alphavantage')
            if tmp is None:
                return None
            tmp = tmp[benchmark]
            benchmark_date = tmp.index.values
            tmp = pd.DataFrame(index=benchmark_date)
            tmp.index = pd.to_datetime(tmp.index, utc=True).tz_localize('UTC')
            tmp = pd.concat([tmp, rtn], axis=1)
            tmp = tmp.ffill().bfill()
            tmp.index.name = rtn.index.name
            rtn = tmp.loc[(tmp.index >= start) & (tmp.index <= end)]
        else:
            date_index = pd.date_range(start, end, freq=calendar_days, tz='UTC')
            tmp = pd.DataFrame(index=date_index)
            tmp = pd.concat([tmp, rtn], axis=1)
            tmp = tmp.ffill().bfill()
            tmp.index.name = rtn.index.name
            rtn = tmp.loc[(tmp.index >= start) & (tmp.index <= end)]
    rtn.index = pd.to_datetime(rtn.index)
    return pd.Panel({symbol: rtn})


def ql_search_symbol(value):
    ''' search a symbol by a string value
    '''
    return MarketDataAgent.search_symbol(value)





# This is the magic part. Find all modules in plugins/ and register them
parent_name = '.'.join(__name__.split('.')[:-1])
parent_module = sys.modules[parent_name]
for importer, modname, ispkg in pkgutil.iter_modules(parent_module.__path__):
    if modname == 'plugins' and ispkg:
        plugin_name = parent_name + '.' + modname
        plugin_module = importlib.import_module(plugin_name)
        for loader, module_name, is_pkg in pkgutil.iter_modules(plugin_module.__path__):
            if not is_pkg:
                loader.find_module(module_name).load_module(module_name)
